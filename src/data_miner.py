import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import pandas_market_calendars as mcal
import redis

from market_calendar import MarketCalendar

# Логгер для этого файла
log = logging.getLogger("data_miner")


class IBError(Exception):
    pass


def get_key(instrument):
    # Всё правильно, в базу бары складываются с ключом TRADES
    return "{sid}:TRADES".format(**instrument)


DT_FMT = "%Y-%m-%d %H:%M:%S"


class DataMiner:
    rc: redis.Redis
    data_delay: int = 0

    # Cколько данных максимум забирать из базы для поиска not final.
    # Не учитывается в режиме load_history_mode.
    load_limit: int = 1000

    # Сколько данных в любом случае забирать.
    # Запас вокруг данных в статусе not final.
    load_margin: int = 5

    load_history_mode: bool = False

    def __init__(self, ib, rc: redis.Redis, schedule: MarketCalendar) -> None:
        self.rc = rc
        self.ib = ib
        self.schedule = schedule

    def update_instrument(self, instrument):
        now = datetime.utcnow().replace(tzinfo=timezone.utc)

        # Сделать минутную сетку
        grid = self.get_grid(instrument, as_of=now)

        # Положить в неё данные из базы.
        grid = self.load_redis_data(grid, instrument)

        # Заполнить пробелы данными из IBKR
        grid = self.fill_ibkr_data(grid, instrument)

        # Сравнить данные из базы и из IBKR, обновить при различиях.
        self.update_db(grid, instrument)

    def get_grid(self, instrument, as_of):
        """
        Минутная сетка с разметкой основной и расширенной биржевой сессии.
        Возвращает сетку, где есть N рабочих минут до now включительно.
        Нерабочие минуты включаются в сетку, но их количество не учитывается.
        """
        working_minutes_cnt = self.load_limit

        exchange = instrument["sid"].split("_")[0]

        # is_open = self.schedule.is_open(symbol, dt)
        # is_rth = self.schedule.is_rth(symbol, dt)

        calendar = self.schedule.get_calendar(exchange)

        # Запас, чтобы покрыть 1000 минут с учетом выходных,
        # иначе будет ошибка "indexer is out-of-bounds" в iloc.
        day = datetime.today().date()
        dt_1 = day - timedelta(days=7)
        dt_2 = day + timedelta(days=3)

        # Режим загрузки исторических данных.
        # Здесь нет ограничений по количеству интервалов в прошлое.
        if self.load_history_mode:
            dt_1 = day - timedelta(days=10)

        # Минутная сетка шкалы времени
        df = pd.DataFrame(pd.date_range(dt_1, dt_2, freq="1T", tz="UTC"))

        # Расписание нужной биржи (все доступные интервалы)
        schedule = calendar.schedule(dt_1, dt_2, market_times="all")

        # Минутные интервалы ETH
        # Если в сетке есть
        times = calendar.regular_market_times
        if "pre" in times and "post" in times:
            schedule[["market_open", "market_close"]] = schedule[["pre", "post"]]
        open = mcal.date_range(schedule, "1T", force_close=True)

        # Смещение на одну минуту нужно, чтобы интервал
        # HH:00 был как следующие интервалы этого часа
        df["open"] = df[0].isin(open).shift(-1)

        df.set_index(0, inplace=True)

        # Обрезать всё после now.
        # Делается запас, чтобы не обрабатывалась открытая минута.
        df = df[: as_of - timedelta(seconds=65)]

        if not self.load_history_mode:
            # Нужное количество интервалов (с конца), где биржа открыта
            start_dt = df[df["open"]].iloc[-working_minutes_cnt].name
            df = df.loc[start_dt:]

        # Unix timestamp, seconds
        df["ts"] = df.index.view("int64") // 10**9

        df["ib"] = None

        return df

    def _final_bar(self, bar):
        s = bar.db if type(bar.db) is str else ""
        return '{"dt":' in s and ('"o":' in s or '"closed":' in s or '"empty":' in s)

    def _has_data(self, bar):
        s = bar.db if type(bar.db) is str else ""
        return '{"dt":' in s and '"o":' in s

    def load_redis_data(self, grid: pd.DataFrame, instrument: dict):
        """
        Данные загружаются из Redis и складываются в поля сетки.
        """
        start_ts = int(grid.ts[0])

        key = get_key(instrument)
        db_data = self.rc.zrangebyscore(key, start_ts, 10**10, withscores=True)
        db_data = [[int(d[1]), d[0]] for d in db_data]

        grid["db"] = grid.ts.map(dict(db_data))

        # Статус интервала из базы
        grid["final"] = grid.apply(self._final_bar, axis=1)
        grid["has_data"] = grid.apply(self._has_data, axis=1)

        return grid

    def get_min_editable_bar_ts(self, grid):
        """
        Интервал не слишком старый для редактирования.

        Иногда IBKR меняет старые данные.
        После закрытия торговой сессии присылают данные премаркета.
        Приходится это игнорировать, т.к. это ломает импорт.
        """
        return int(grid.ts[-1]) - 3600 * 1

    def validate_ibkr_res(self, res):
        """
        Валидация ответа.
        """
        if res.error or res.exception:
            raise IBError(res.error or "exception")
        if not res.json:
            raise IBError("no_json")
        if not res.json.get("data"):
            raise IBError("no_data")

    def load_ibkr_data(self, instrument: dict, to_load: int, from_ts: int):
        """
        Запросить данные из IBKR, начиная с первого пробела.
        Делается несколько попыток с минимальным перерывом.
        """

        to_load_sec = to_load * 60
        duration = f"{to_load_sec} S"

        res = {"data": []}

        log.info(
            f"TO LOAD: {instrument['sid']}, "
            f"minutes: {to_load}, from: {from_ts}, "
            f"duration: {duration}"
        )

        contract = self.ib.contract_for_sid(instrument["sid"])

        data_type = "TRADES"
        if contract.secType in ["CASH"]:
            data_type = "MIDPOINT"

        if self.load_history_mode:
            ib_res = []
            duration = "86400 S"
            for i in range(10):
                if ib_res:
                    ts = int(ib_res[0].date)
                    if ts < from_ts:
                        break
                    t1 = datetime.utcfromtimestamp(ts)
                    end_dt = t1.strftime("%Y%m%d-%H:%M:%S")
                    log.info(f"Loading history ({i})... end_dt: {end_dt}")
                else:
                    end_dt = ""
                ib_res = (
                    self.ib.get_historical_data(
                        contract,
                        data_type=data_type,
                        end_dt=end_dt,
                        duration=duration
                    )
                    + ib_res
                )
        else:
            ib_res = self.ib.get_historical_data(
                contract,
                data_type=data_type,
                end_dt="",
                duration=duration
            )

        log.info(f"Loaded from IB: {len(ib_res)}")

        # результат выдать в виде json bar
        ib_data = []
        for line in ib_res:
            bar = {
                "o": line.open,
                "h": line.high,
                "l": line.low,
                "c": line.close,
                "v": round(float(line.volume), 2),
            }
            ib_data.append((int(line.date), bar))

        res["data"] = ib_data
        return res

    def fill_ibkr_data(self, grid: pd.DataFrame, instrument: dict):
        # Найти рабочие интервалы без окончательных данных
        grid_not_final = grid[(grid.final != True) & (grid.open == True)]

        if grid_not_final.empty:
            # Ничего грузить не нужно, сетка заполнена
            log.info(f"Grid is full for {instrument}")
            return grid

        # Для корректной работы Empty FSM нужно при загрузке из IB зацепить
        # интервал с данными. Следовательно, для акций (с премаркетом)
        # нужно доматывать в прошлое до последнего интервала до перерыва.
        grid_has_data = grid[(grid.has_data == True) & (grid.open == True)]

        # Посчитать количество интервалов, которые нужно загрузить

        first_not_final_ts = grid_not_final.ts[0]

        if grid_has_data.empty:
            last_data_ts = None
        else:
            last_data_ts = grid_has_data.ts[-1]

        first_to_load_ts = first_not_final_ts

        # Для акций учитывается последний интервал с данными, если он был.
        # FIXME: корректно определить тип stocks
        if instrument["sid"].count("_") == 1 and "PAXOS" not in instrument["sid"]:
            if last_data_ts:
                first_to_load_ts = min(first_not_final_ts, last_data_ts)

        to_load = grid[grid.ts >= first_to_load_ts].shape[0]
        to_load = min(to_load + self.load_margin, self.load_limit)

        # Попытка загрузки данных из IBKR
        if res := self.load_ibkr_data(instrument, to_load, first_to_load_ts):
            # TODO: поддержка этого
            # Время задержки данных для аккаунта без подписки
            # self.data_delay = res.json.get("mktDataDelay") or 0

            # if self.data_delay > 0:
            #     log.debug(f"Data delay: {self.data_delay} seconds")
            #     self.data_delay += 100

            # Положить данные IB в сетку, матчинг по полю ts
            grid["ib"] = grid.ts.map(dict(res["data"]))

        return grid

    def _empty_bar_fsm(self, empty_bar_state, row):
        """
        Empty bar validation FSM.

        Определяем, является ли отсутствие данных признаком ошибки,
        или сделок не было в начале торгового дня (премаркет акций).

        Сначала empty_bar_state = None.
        Если интервал из IB содержит данные, то ставим has_data.
        После этого можно делать какие-то выводы про Error vs Empty.

        Если в последующих интервалах данных нет...
            и биржа закрыта >> closed
            и биржа открыта, но до этого была закрыта >> empty_ok

        Если пришли данные, то FSM возвращается к началу.

        Для корректной работы нужно зацепить интервал с данными.
        Следовательно, для акций (с премаркетом) нужно доматывать
        в прошлое до последнего рабочего интервала.
        """
        if row.ib and type(row.ib) is dict:
            empty_bar_state = "has_data"
        if empty_bar_state and not (row.ib and type(row.ib) is dict):
            if not row.open:
                empty_bar_state = "closed"
            elif empty_bar_state == "closed":
                empty_bar_state = "empty_ok"
        return empty_bar_state

    def update_db(self, grid: pd.DataFrame, instrument: dict):
        """
        closed — биржа закрыта
        empty  — биржа открыта, но сделок сегодня еще не было
        fix    — интервал был перезаписан
        error  — ошибка
        """
        # Замена всякой хуйни на None
        grid = grid.where(pd.notnull(grid), None)

        # FSM for possibility of empty bar state
        empty_bar_state = None

        # min_editable_bar_ts = self.get_min_editable_bar_ts(grid)

        for row in grid.itertuples():
            # Empty bar FSM needs full grid (with final bars)
            empty_bar_state = self._empty_bar_fsm(empty_bar_state, row)

            # Закомментировано, т.к. задача решается через row.final
            # # Запрет редактирования старых интервалов,
            # # для которых в базе уже есть что-то осмысленное
            # if row.db and type(row.db) is str and row.ts < min_editable_bar_ts:
            #     ...

            if row.final:
                continue

            if not row.open and row.ib:
                log.error(f"IBKR bar data on closed market {row.ib}")

            late = row.ts < (grid.ts[-1] - self.data_delay)

            if not row.open:
                # Биржа закрыта
                bar = {"closed": 1}
            elif row.ib and type(row.ib) is dict:
                # Есть нормальный интервал
                bar = row.ib.copy()
                if late and not self.load_history_mode:
                    bar["late"] = 1
            elif empty_bar_state == "empty_ok":
                # Корректные условия для EMPTY
                bar = {"empty": 1}
            else:
                # Данные должны быть, но их нет
                if late and empty_bar_state:
                    if empty_bar_state:
                        bar = {"error": 1}
                    else:
                        # похоже, интервал слишком старый и не влез в лимит
                        log.debug(f"Skip old empty bar: {row}")
                        continue
                else:
                    bar = {"delay": 1}

            if row.db and type(row.db) is str:
                if "error" in bar:
                    log.debug(f"Don't rewrite with error. Old: {row.db}")
                    continue
                elif "delay" in row.db:
                    log.debug(f"Don't mark delay as a fix. Old: {row.db}")
                else:
                    bar["fix"] = 1

            self.save_historical_bar(instrument, bar, row)

    def save_historical_bar(self, instrument, bar, row):
        """
        Запись в базу с заменой старых данных.
        """
        # Добавить дату
        dt = row.Index.to_pydatetime()
        dt_str = row.Index.strftime(DT_FMT)
        bar_data = dict(dt=dt_str, **bar)

        sid = instrument["sid"]
        key = get_key(instrument)

        msg_data = bar_data.copy()
        msg_data["sid"] = sid

        msg_str = json.dumps(msg_data, indent=None, default=str)
        bar_str = json.dumps(bar_data, separators=(",", ":"))

        # Не сохранять такую же строку повторно (не учитывая флаг fix)
        # FIXME: выглядит тупо
        if str(row.db).replace(',"fix":1', "") == bar_str.replace(',"fix":1', ""):
            return

        log.info(f"{key} {bar_str}, old: {row.db}")

        self.rc.zremrangebyscore(key, row.ts, row.ts)
        self.rc.zadd(key, {bar_str: row.ts})

        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        diff = (now - dt).total_seconds()

        # Данные за последние 10 минут отправляются в REDIS
        if not self.load_history_mode and diff < 600:
            a = self.rc.publish(f"{sid}:BARS", msg_str)
            log.info(f"Redis 1-min [miner]: {msg_str} - {a}")
