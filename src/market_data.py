import json
import logging
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from time import monotonic, sleep
from zoneinfo import ZoneInfo

import coloredlogs
import redis
from ib_sync import IBSync, IBThread
from ibapi.common import BarData
from termcolor import colored

from data_miner import DataMiner
from market_calendar import MarketCalendar
from settings import AppConfig, app_config

# Логгер для этого файла
log = logging.getLogger("tradis")

# loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
# for logger in loggers:
#     logger.setLevel(logging.INFO)

# logging.getLogger("ibapi.connection").setLevel(logging.DEBUG)


coloredlogs.install(
    "INFO", fmt="%(asctime).19s • %(levelname).1s • %(name)s • %(message)s"
)


DT_FMT = "%Y-%m-%d %H:%M:%S"


def ts_to_dt(ts):
    return datetime.utcfromtimestamp(ts)


def is_redis_available(r):
    try:
        r.ping()
    except Exception:
        return False
    return True


class FatalException(Exception):
    """
    Ошибка, после которой нужен переконнект.
    """

    pass


class IBSyncData(IBSync):
    """
    Версия IB-клиента для работы с историческими данными.
    """

    def __init__(self, redis_client, instruments):
        super().__init__()
        self.instruments = instruments
        self.prev_bar = {}  # last bar by r_id
        self.request = {}  # request data by r_id
        self.rc = redis_client
        self.connections = {
            "tws": "disconnected",
            "ibkr": "",
        }
        self.response_dt = datetime.min

    def save_bar(self, bar, sid):
        """
        Запись в базу с заменой старых данных.
        """
        # Добавить дату
        ts = int(bar.date)
        dt_str = ts_to_dt(ts).strftime(DT_FMT)
        # bar = dict(dt=dt_str, **bar)

        key = f"{sid}:TRADES"

        bar_data = {
            "dt": dt_str,
            "o": bar.open,
            "h": bar.high,
            "l": bar.low,
            "c": bar.close,
            "v": round(float(bar.volume), 2),
        }
        msg_data = bar_data.copy()
        msg_data["sid"] = sid

        msg_str = json.dumps(msg_data, indent=None, default=str)
        bar_str = json.dumps(bar_data, separators=(",", ":"))

        self.rc.zremrangebyscore(key, ts, ts)
        self.rc.zadd(key, {bar_str: ts})

        a = self.rc.publish(f"{sid}:BARS", msg_str)
        log.info(f"Redis 1-min: {msg_str} - {a}")

    def realtimeBar(
        self,
        reqId: int,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Decimal,
        wap: Decimal,
        count: int,
    ):
        """
        5-sec real-time OHLC bars.
        """
        # log.info(f"realtime Bar: r_id: {reqId}")
        if req := self.request.get(reqId):
            req["responce_dt"] = datetime.utcnow()

        dt = ts_to_dt(time)
        sid = self.request[reqId]["sid"]

        prices = list(set([open_, high, low, close]))
        for price in prices:
            msg = {
                "dt": dt.strftime(DT_FMT),
                "sid": sid,
                "price": price,
                # TODO: добавить volume
            }
            json_str = json.dumps(msg, indent=None, default=str)
            a = self.rc.publish(f"{sid}:TRADES", json_str)
            # log.info(f"Redis 5-sec: {json_str} - {a}")

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        """
        При появлении нового бара отправить старый бар в Redis
        """
        # log.info(f"historical Bar: r_id: {reqId}")
        if req := self.request.get(reqId):
            req["responce_dt"] = datetime.utcnow()

            # Если пришел бар по отмененной подписке - отменить еще раз
            if req.get("cancelled"):
                log.warning(f"cancelHistoricalData AGAIN: {req}")
                self.cancelHistoricalData(reqId)
                return

        last_bar = self.prev_bar.get(reqId)
        sid = self.request[reqId]["sid"]

        if last_bar and last_bar.date < bar.date:
            self.save_bar(last_bar, sid)

        self.prev_bar[reqId] = bar

    def managedAccounts(self, accountsList: str):
        super().managedAccounts(accountsList)
        self.connections["tws"] = "connected"
        self.response_dt = datetime.utcnow()

    def currentTime(self, time):
        super().currentTime(time)
        self.connections["tws"] = "connected"
        self.response_dt = datetime.utcnow()

    def connectionClosed(self):
        super().connectionClosed()
        # Все подписки сбрасываются, когда соединение закрывается
        self.connections["tws"] = "disconnected"
        for r_id, sub in self.request.items():
            if not sub.get("cancelled"):
                log.error(f"Subscription cancelled: {r_id} {sub['sid']}")
                self.request[r_id]["cancelled"] = True

    def error(self, reqId: int, errorCode: int, errorString: str, ordRejectJson=""):
        super().error(reqId, errorCode, errorString, ordRejectJson)
        connection_updated = False

        # TODO: можно еще обработать 165
        # Historical Market Data Service query message:HMDS connection attempt failed.
        # Обработать ошибку отмены
        # code: 366, msg: No historical data query found for ticker id:22980429

        # connected
        if errorCode in [2104, 2106, 2158]:
            source = errorString.split("connection is OK:")[1]
            source = source.strip()
            self.connections[source] = "connected"
            connection_updated = True

        # mass reconnection, data maintained
        # FIXME: обработать not connected
        # The following farms are connected: usfuture; usfarm; secdefnj.
        # The following farms are not connected: ushmds.
        if errorCode == 1102:
            txt = errorString.split("are connected:")[1]
            for source in txt.split(";"):
                source = source.strip().strip(".")
                self.connections[source] = "connected"
            self.connections["ibkr"] = "connected"
            connection_updated = True

        # mass reconnection, data lost
        if errorCode == 1101:
            for key in self.connections.keys():
                self.connections[key] = "disconnected"
            self.connections["ibkr"] = "connected"
            connection_updated = True

        # disconnected
        if errorCode in [2103, 2105, 2157]:
            source = errorString.split("connection is broken:")[1]
            source = source.strip()
            self.connections[source] = "disconnected"
            connection_updated = True

        # inactive
        if errorCode in [2107, 2108]:
            source = errorString.split("upon demand.")[1]
            source = source.strip()
            self.connections[source] = "inactive"
            connection_updated = True

        # connecting (undocumented)
        if errorCode in [2119]:
            source = errorString.split("is connecting:")[1]
            source = source.strip()
            self.connections[source] = "connecting"
            connection_updated = True

        # IB disconnected
        if errorCode in [1100, 2110]:
            self.connections["ibkr"] = "disconnected"
            connection_updated = True

        if connection_updated:
            self.connections["tws"] = "connected"

        # Это приходит без связи с tws
        if errorCode in [502, 504, 1300]:
            for key in self.connections.keys():
                self.connections[key] = "disconnected"
            self.connections["ibkr"] = ""
            connection_updated = True

        # Failed to request live updates (disconnected)
        if errorCode == 10182:
            sub = self.request.get(reqId)
            if sub and not sub.get("cancelled"):
                log.error(f"Subscription cancelled: {reqId} {sub['sid']}")
                sub["cancelled"] = True

    def unsubscribe_if_active(self, sid, request_type):
        # Отменить активные подписки данного вида
        for r_id, sub in self.request.items():
            if sub["sid"] == sid and sub["request_type"] == request_type:
                if not sub.get("cancelled"):
                    if sub["request_type"] == "real_time_bars":
                        log.warning(f"cancelRealTimeBars: {r_id}")
                        self.cancelRealTimeBars(r_id)
                        self.request[r_id]["cancelled"] = True
                        sleep(1)
                    if sub["request_type"] == "historical":
                        log.warning(f"cancelHistoricalData: {r_id}")
                        self.cancelHistoricalData(r_id)
                        self.request[r_id]["cancelled"] = True
                        sleep(1)

    def subscribe(self, sid, request_type):
        contract = self.contract_for_sid(sid)

        self.unsubscribe_if_active(sid, request_type)

        data_type = "TRADES"
        if request_type == "historical" and contract.secType in ["CRYPTO", "CASH"]:
            data_type = "MIDPOINT"

        r_id = self.r_id
        self.request[r_id] = {
            "request_type": request_type,
            "contract": contract,
            "sid": sid,
            "data_type": data_type,
            "responce_dt": datetime.utcnow(),
        }
        txt = f"Subscribe: {r_id} {sid} {request_type}"
        log.info(colored(txt, "green", attrs=["bold"]))

        # Подписка на 5-sec интервалы
        if request_type == "real_time_bars":
            self.reqRealTimeBars(r_id, contract, 5, data_type, False, [])

        # Подписка на 1-min интервалы
        if request_type == "historical":
            self.reqHistoricalData(
                r_id,
                contract,
                endDateTime="",
                durationStr="300 S",
                barSizeSetting="1 min",
                whatToShow=data_type,
                useRTH=0,
                formatDate=2,
                keepUpToDate=True,
                chartOptions=[],
            )


class Tradis:
    def __init__(self, config: AppConfig) -> None:
        self.redis_config = config.redis
        self.gateway = config.gateway
        self.instruments = config.instruments
        self.history = config.history

        self.running = True
        self.in_long_break = None

        self.request_time = datetime.min
        self.prev_miner = datetime.min

        log.info(colored("Start Tradis ⋅ﾐ(•ᵕ•)ﾉ", "magenta"))
        log.info(f"Gateway: {self.gateway}")
        log.info(f"Redis: {self.redis_config}")
        log.info(f"History: {self.history}")

        self.subscriptions = []
        sids = []
        for instrument in self.instruments:
            sid = instrument["sid"]
            sids.append(sid)
            self.subscriptions.append({"sid": sid, "request_type": "real_time_bars"})
            self.subscriptions.append({"sid": sid, "request_type": "historical"})

        self.schedule = MarketCalendar(sids, datetime.utcnow())

        self.rc = redis.Redis(**dict(self.redis_config))
        self.ib = IBSyncData(self.rc, self.subscriptions)
        self.last_known_connections_status = str(self.ib.connections)

    def request_tws_time(self):
        self.request_time = datetime.utcnow()
        self.ib.reqCurrentTime()

    def print_connection_status(self):
        """
        Нарядная строка статуса соединений.
        """
        res = []
        marks = {
            "connected": "🟩",
            "connecting": "🟡",
            "disconnected": "❌",
            "inactive": "⚪",
            "?": "🔲",
        }
        for key, value in self.ib.connections.items():
            mark = marks.get(value, marks["?"])
            res.append(f"{mark} {key}")
        log.info(" ⋅ ".join(res))

    def get_delayed(self) -> list[tuple]:
        """
        Если у активного запроса последний ответ был больше N секунд назад
        и рынок по данному инструменту открыт, добавить подписку в delayed.
        """
        delayed = []
        max_delay = 100

        for r_id, req in self.ib.request.items():
            dt = datetime.utcnow()
            delay = (dt - req["responce_dt"]).total_seconds()

            if req.get("cancelled") or delay < max_delay:
                continue

            rt = req["request_type"]
            txt = f"Delayed: {r_id}, {req['sid']}, {rt}, {delay:0.1f} sec"

            # В начале торговой сессии всё delayed,
            # поэтому смотрю расписание на предыдущую минуту.
            schedule_dt = dt - timedelta(minutes=1)

            if self.schedule.is_rth(req["sid"], schedule_dt):
                log.warning(txt + " - resubscribe")
                delayed.append((req["sid"], rt))
            # else:
            #     log.warning(txt + " - CLOSED, SKIP")

        return delayed

    def get_inactive(self) -> list[tuple]:
        """
        Для каждой подписки найти соответствующий запрос в IB.
        Если нет активной подписки, добавить в inactive.
        """
        inactive = []
        for sub in self.subscriptions:
            # поискать такое в активных запросах
            has_active = False
            for req in self.ib.request.values():
                if (
                    req["sid"] == sub["sid"]
                    and req["request_type"] == sub["request_type"]
                    and not req.get("cancelled")
                ):
                    has_active = True
            if not has_active:
                inactive.append((sub["sid"], sub["request_type"]))
        return inactive

    def ibkr_farms_connected(self):
        connected = True
        farms = ["secdefnj", "ushmds", "usfarm"]
        for farm in farms:
            if self.ib.connections.get(farm) not in ["connected", "inactive"]:
                connected = False
                break
        return connected

    def maintain(self) -> None:
        """
        Проверка статуса подписок и задержки прихода данных.
        """

        # Данные приходят с задержкой
        delayed = self.get_delayed()

        # Нет активной подписки
        inactive = self.get_inactive()

        # Проверить синхронизацию времени
        time_diff = abs((self.request_time - self.ib.tws_time).total_seconds())
        if time_diff > 100:
            log.error(f"TWS time out of sync: {time_diff:0.2f} sec")
        elif time_diff > 10:
            log.warning(f"TWS time out of sync: {time_diff:0.2f} sec")

        # Новый запрос времени
        self.request_tws_time()

        if not self.ib.isConnected():
            raise FatalException("tws_disconnected")

        # Проверить, когда от TWS последний раз приходил ответ
        response_gap = (datetime.utcnow() - self.ib.response_dt).total_seconds()
        if response_gap > 100:
            raise FatalException("tws_delay")
        elif response_gap > 20:  # сильно больше, чем период maintain
            log.warning(f"Large TWS response gap: {response_gap:0.2f} sec")

        if need_reconnection := sorted(set(inactive + delayed)):
            # Не запускать подписки, если фермы с данными disconnected
            if self.ibkr_farms_connected():
                # Переподписка на всё, что не работает как надо
                for sub in need_reconnection:
                    self.ib.subscribe(sub[0], sub[1])
            else:
                log.info("No farm connections, SKIP")

    def reset(self):
        # Сбросить все статусы подписки на данные
        pass

    def periodic_actions(self) -> None:
        # Проверка связи с Redis
        if not is_redis_available(self.rc):
            raise FatalException("redis_unavailable")

        str_connections = json.dumps(self.ib.connections)
        self.rc.set("connections", str_connections)

        # Вывести строку статусов, если они изменились
        if self.last_known_connections_status != str_connections:
            self.print_connection_status()
            self.last_known_connections_status = str_connections

        # Проверки соединения и подписок на данные
        if monotonic() - self.prev_maintain > 5:
            self.prev_maintain = monotonic()
            self.maintain()

    def try_data_miner(self) -> None:
        """
        Проверка и заполнение сетки в базе.
        """
        dt = datetime.utcnow()
        # Это новая минута и прошло достаточно секунд от начала
        if dt.minute != self.prev_miner.minute and dt.second > 30:
            self.prev_miner = dt

            if self.in_long_break is False:
                self.print_connection_status()

            dm = DataMiner(self.ib, self.rc, self.schedule)
            dm.load_history_mode = self.history

            online = self.ibkr_farms_connected() and self.in_long_break is False
            if online:
                log.info(colored("DataMiner update", "green", attrs=["bold"]))
            else:
                log.info(colored("DataMiner offline", "red", attrs=["bold"]))

            for instrument in self.instruments:
                try:
                    dm.update_instrument(instrument, online)
                except Exception as e:
                    log.error(f"ERROR in DataMiner: {e}")
                    log.exception(e)

            log.info(colored("DataMiner done", attrs=["bold"]))

    def long_break(self) -> bool:
        """
        Пятничный долгий перерыв IBKR.
        Часовой пояс Los Angeles, чтобы перерыв поместился в один день.
        Вообще он с 20, но после 17 всё равно ничего не работает.
        """
        now = datetime.now(ZoneInfo("America/Los_Angeles"))
        return now.isoweekday() == 5 and now.hour >= 19 and now.minute >= 30

    def run(self):
        """
        Бесконечный цикл, в котором поддерживаются нужные
        соединения с TWS/GW и нужные подписки на данные.
        """
        while not sleep(0.1) and self.running:
            # В любом случае можно запустить DataMiner
            self.try_data_miner()

            # Во время большого перерыва ничего не делать
            if self.long_break():
                if not self.in_long_break:
                    log.warning("Enter IBKR long break")
                    self.in_long_break = True
                    self.ib.disconnect()
                continue
            else:
                if self.in_long_break is None:
                    self.in_long_break = False
                if self.in_long_break is True:
                    log.warning("Exit IBKR long break")
                    self.in_long_break = False

            # Попытка дисконнекта, если есть чего
            try:
                if self.ib:
                    self.ib.disconnect()
            except Exception as e:
                log.error(f"TWS disconnect exception: {e}")

            # Подключение к TWS
            try:
                host = self.gateway.host
                port = self.gateway.port
                client_id = self.gateway.client_id
                self.ib.tws_time = datetime.min
                self.ib.connect(host, port, client_id)
            except Exception as e:
                log.error(f"TWS connect exception: {e}")
                log.exception(e)
                self.ib.disconnect()
                sleep(5)
                continue

            # Если соединение есть, но отваливается, значит client_id занят
            if self.ib.isConnected():
                dt = monotonic()
                while not sleep(0.2) and monotonic() - dt < 2:
                    if not self.ib.isConnected():
                        log.error(f"Possibly client_id conflict: {client_id}")
                        break

            # Если TWS не запущен или в процессе перезапуска,
            # isConnected вернет false. Повторить попытку через N секунд
            if not self.ib.isConnected():
                log.error("No TWS connection, reconnect in 20 sec")
                sleep(20)
                continue

            # Поток обработки входящих сообщений
            try:
                IBThread(self.ib).start()
            except Exception as e:
                log.exception(e)
                log.error("IBThread exception, reconnect in 5 sec")
                sleep(5)
                continue

            # You have to make sure the connection has been fully established
            # before attempting to do any requests to the TWS.
            # Failure to do so will result in the TWS closing the connection.
            dt = monotonic()
            while not sleep(0.2) and monotonic() - dt < 5:
                if self.ib.nextValidOrderId > 0:
                    log.info(f"TWS is connected, order id: {self.ib.nextValidOrderId}")
                    break

            # Если не дождались - переконнект
            if not self.ib.nextValidOrderId > 0:
                log.error("No TWS connection, no Next Order ID, reconnect now")
                continue

            # В этом месте должно быть активное подключение
            self.request_tws_time()

            # Отсечки времени для periodic_actions
            self.prev_maintain = monotonic()

            while not sleep(0.1) and self.ib.isConnected():
                if self.long_break():
                    break

                try:
                    # Операции, которые нужно постоянно повторять
                    self.periodic_actions()
                    self.try_data_miner()

                except (KeyboardInterrupt, SystemExit) as e:
                    raise e

                except FatalException as e:
                    log.error(f"Fatal exception: {e}")
                    break

                except Exception as e:
                    # Что-то пошло не так, но соединение активно.
                    log.error(f"Worker exception: {e}")
                    log.exception(e)


if __name__ == "__main__":
    tradis = Tradis(app_config)

    while True:
        try:
            tradis.run()
        except (KeyboardInterrupt, SystemExit):
            print()
            tradis.ib.disconnect()  # приведет к остановке msg_thread
            log.info(f"DONE")
            break
        except Exception as e:
            # Что-то пошло не так очень глобально.
            log.error(f"Tradis run exception: {e}")
            log.exception(e)
            sleep(5)

    # Для отладки выводится список активных потоков
    for thread in threading.enumerate():
        log.warning(f"Thread alive: {thread}")
