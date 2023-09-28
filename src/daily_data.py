import json
import logging
import threading
from datetime import datetime, timedelta
from time import monotonic, sleep
from zoneinfo import ZoneInfo

import coloredlogs
import redis
from ib_sync import IBSync, IBThread
from termcolor import colored

from market_data import FatalException
from settings import AppConfig, app_config

# Логгер для этого файла
log = logging.getLogger("daily_data")

coloredlogs.install(
    "INFO", fmt="%(asctime).19s • %(levelname).1s • %(name)s • %(message)s"
)


DT_FMT = "%Y-%m-%d"

#
# Это скрипт для сбора дневных OHLC-данных по всем инструментам из конфига.
# Можно запускать по расписанию (раз в час, например). Скрипт достает данные
# за последний год и перезаписывает все дни, которые были получены.
# Скрипт не будет получать данные во время перерыва IBKR.
#


class TradisDaily:
    def __init__(self, config: AppConfig) -> None:
        self.redis_config = config.redis
        self.gateway = config.gateway
        self.instruments = config.instruments

        self.running = True
        self.in_long_break = None

        self.request_time = datetime.min
        self.prev_miner = datetime.min

        # Сдвигается относительно конфига, чтобы не мешать
        self.gateway.client_id += 10

        log.info(colored("Start Tradis for Daily Data ⋅ﾐ(•ᵕ•)ﾉ", "magenta"))
        log.info(f"Gateway: {self.gateway}")
        log.info(f"Redis: {self.redis_config}")

        self.rc = redis.Redis(**dict(self.redis_config))
        self.ib = IBSync()

    def request_tws_time(self):
        self.request_time = datetime.utcnow()
        self.ib.reqCurrentTime()

    def collect_data(self):
        today = (datetime.utcnow() + timedelta(days=1)).date()

        for instrument in self.instruments:
            self.collect_one_sid(instrument["sid"], today)
            sleep(0.1)

    def collect_one_sid(self, sid, end_dt):
        contract = self.ib.contract_for_sid(sid)
        contract.includeExpired = True

        end_dt_str = end_dt.strftime("%Y%m%d 00:00:00 UTC")

        log.info(f"Processing: {sid}, end_dt: {end_dt_str}")

        # details = ib.qualify_contract(contract)
        # print(json.dumps(details.__dict__, default=str, indent=4))
        # print(json.dumps(details.details.__dict__, default=str, indent=4))

        hist = self.ib.get_historical_data(
            contract,
            end_dt=end_dt_str,
            duration="1 Y",
            bar_size="1 day",
            use_rth=False,
        )
        # hts = self.ib.get_head_timestamp(contract)
        # print(hts)  # 1600128000

        ib_data = self.format_data(sid, hist)

        key = f"{sid}:DAILY"

        for score, bar_data in ib_data:
            # записать данные в redis
            bar_str = json.dumps(bar_data, separators=(",", ":"))
            self.rc.zremrangebyscore(key, score, score)
            self.rc.zadd(key, {bar_str: score})

    def format_data(self, sid, ib_res):
        # результат выдать в виде json bar
        ib_data = []
        for line in ib_res:
            d = line.date
            date = d[0:4] + "-" + d[4:6] + "-" + d[6:]
            bar = {
                "d": date,
                "o": line.open,
                "h": line.high,
                "l": line.low,
                "c": line.close,
                "v": round(float(line.volume), 2),
            }
            ib_data.append((int(line.date), bar))
        return ib_data

    def long_break(self) -> bool:
        """
        Пятничный долгий перерыв IBKR.
        Часовой пояс Los Angeles, чтобы перерыв поместился в один день.
        Вообще он с 20, но после 17 всё равно ничего не работает.
        """
        now = datetime.now(ZoneInfo("America/Los_Angeles"))
        return now.isoweekday() == 5 and now.hour >= 19

    def run(self):
        """
        Бесконечный цикл, в котором поддерживаются нужные
        соединения с TWS/GW и нужные подписки на данные.
        """
        while not sleep(0.1) and self.running:
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

            # Поддержка delayed data
            self.ib.reqMarketDataType(3)
            sleep(0.5)

            # Отсечки времени для periodic_actions
            self.prev_maintain = monotonic()

            if self.ib.isConnected():
                if self.long_break():
                    break

                try:
                    # Операции, которые нужно постоянно повторять
                    self.collect_data()

                except (KeyboardInterrupt, SystemExit) as e:
                    raise e

                except FatalException as e:
                    log.error(f"Fatal exception: {e}")
                    break

                except Exception as e:
                    # Что-то пошло не так, но соединение активно.
                    log.error(f"Worker exception: {e}")
                    log.exception(e)

            try:
                if self.ib:
                    self.ib.disconnect()
            except Exception as e:
                log.error(f"TWS disconnect exception: {e}")

            log.info("run DONE")
            break


if __name__ == "__main__":
    tradis = TradisDaily(app_config)
    try:
        tradis.run()
    except (KeyboardInterrupt, SystemExit):
        print()
        tradis.ib.disconnect()  # приведет к остановке msg_thread
        log.info(f"DONE")
    except Exception as e:
        # Что-то пошло не так очень глобально.
        log.error(f"Tradis run exception: {e}")
        log.exception(e)

    sleep(1)

    # Для отладки выводится список активных потоков
    for thread in threading.enumerate():
        log.warning(f"Thread alive: {thread}")
