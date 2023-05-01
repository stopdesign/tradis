from abc import ABC
from datetime import datetime, time, timedelta, timezone

import pandas_market_calendars as mcal
import pytz
from pandas import date_range
from pandas.tseries.offsets import CustomBusinessDay
from pandas_market_calendars import MarketCalendar as PandasMarketCalendar


class CustomCBOT(mcal.exchange_calendar_cme.CMEAgricultureExchangeCalendar):
    # regular_market_times = {
    #     "market_open": ((None, time(8,30)),),
    #     "market_close": ((None, time(13,20)),),
    # }
    regular_market_times = {
        "market_open": ((None, time(19), -1),),
        "market_close": ((None, time(13, 20)),),
        "break_start": ((None, time(7, 45)),),
        "break_end": ((None, time(8, 30)),),
    }

    @property
    def tz(self):
        return pytz.timezone("America/Chicago")


class RiseCBOT(mcal.exchange_calendar_cme.CMEAgricultureExchangeCalendar):
    regular_market_times = {
        "market_open": ((None, time(8, 30)),),
        "market_close": ((None, time(21, 0)),),
        "break_start": ((None, time(13, 20)),),
        "break_end": ((None, time(19, 0)),),
    }

    @property
    def tz(self):
        return pytz.timezone("America/Chicago")


mask_sun = CustomBusinessDay(weekmask="Sun")  # type: ignore
sundays = date_range("2020-01-01", "2030-01-01", freq=mask_sun)


class CustomPaxos(PandasMarketCalendar, ABC):
    """
    Расписание для крипты в IB.
    """

    regular_market_times = {
        "market_open": ((None, time(16, 1), -1),),
        "market_close": ((None, time(16)),),
    }

    @property
    def name(self):
        return "Paxos"

    @property
    def weekmask(self):
        return "Mon Tue Wed Thu Fri Sun"

    @property
    def special_opens_adhoc(self):
        return [
            (time(3), sundays),
        ]

    @property
    def tz(self):
        return pytz.timezone("US/Eastern")


class Forex(PandasMarketCalendar, ABC):
    """
    Расписание Forex.
    """

    regular_market_times = {
        "market_open": ((None, time(16, 15), -1),),
        "market_close": ((None, time(16)),),
    }

    @property
    def name(self):
        return "Forex"

    @property
    def tz(self):
        return pytz.timezone("America/Chicago")


IBKR_TO_MCAL = {
    "NASDAQ": "NASDAQ",
    "NYMEX": "CMEGlobex_NatGas",  # FIXME: разный режим для разных инструментов
    "NYSE": "NYSE",
    "ARCA": "NYSE",
    "CME": "CME_Rate",
    "IDEALPRO": "Forex",
    "CBOT": "CustomCBOT",
    "PAXOS": "CustomPaxos",
}


def get_mcal_exchange(sid):
    exchange, rest = sid.split("_", 1)
    symbol = rest.split("_")[0]
    if exchange == "CBOT" and symbol == "ZR":
        exchange = "RiseCBOT"
    if exchange in IBKR_TO_MCAL:
        exchange = IBKR_TO_MCAL[exchange]
    return exchange


def dt_to_ts(dt):
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


class MarketCalendar:
    def __init__(self, instruments, dt_1, dt_2: datetime | None = None) -> None:
        self.dt_1 = dt_1 - timedelta(days=15)
        if dt_2:
            self.dt_2 = dt_2 + timedelta(days=15)
        else:
            self.dt_2 = datetime.today() + timedelta(days=300)
        self.grids = self.init_grids(instruments)

    def init_grids(self, instruments):
        res = {}
        for sid in instruments:
            exchange = get_mcal_exchange(sid)
            if exchange in res:
                continue
            calendar = self.get_calendar(sid)
            res[exchange] = self._get_grid(calendar)
        return res

    def get_calendar(self, sid: str) -> PandasMarketCalendar:
        exchange = get_mcal_exchange(sid)
        if exchange in IBKR_TO_MCAL:
            exchange = IBKR_TO_MCAL[exchange]
        if exchange == "CustomCBOT":
            calendar = CustomCBOT()
        elif exchange == "CustomPaxos":
            calendar = CustomPaxos()
        elif exchange == "RiseCBOT":
            calendar = RiseCBOT()
        else:
            calendar = mcal.get_calendar(exchange)
        return calendar

    def _get_grid(self, calendar: PandasMarketCalendar):
        """
        Рабочие минутные интервалы от dt_1 до dt_2 в виде списка timestamps.
        """
        # Расписание нужной биржи (все доступные интервалы)
        schedule = calendar.schedule(self.dt_1, self.dt_2)  # , market_times="all")

        # Минутные интервалы ETH
        # times = calendar.regular_market_times
        # if "pre" in times and "post" in times:
        #     schedule[["market_open", "market_close"]] = schedule[["pre", "post"]]

        # Минутные интервалы RTH
        open = mcal.date_range(schedule, "1T", force_close=1)

        # Смещение на одну минуту нужно, чтобы интервал
        # HH:00 был как следующие интервалы этого часа
        res = {dt - 60 for dt in set(open.view("int64") // 10**9)}

        return res

    def is_rth(self, sid, dt) -> bool:
        exchange = get_mcal_exchange(sid)
        return dt_to_ts(dt.replace(second=0, microsecond=0)) in self.grids[exchange]
