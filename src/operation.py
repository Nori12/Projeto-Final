from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import sys

import constants as c
from utils import State

# Configure Logging
logger = logging.getLogger(__name__)
log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME
file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)


class Operation:
    """
    Operation object for handling `Strategy` attempts of purchase and sale.

    An Operation starts with the purchase of stocks and ends with its complete sale.

    Support ONE purchase and multiple sales.
    Do not support short operations.

    Args
    ----------
    ticker : str
        Ticker name.

    Properties
    ----------
    ticker : str
        Ticker name.
    state : `utils.State`
        Operation state (open, close or not started).
    start_date : `datetime.date`
        Date of the first purchase.
    end_date : `datetime.date`
        Date of the last sale.
    number_of_orders : int
        Total number of purchase or sale orders.
    profit : float
        Profit after operation close.
    result_yield : float
        Yield after operation close.
    target_purchase_price : float
        Target purchase price.
    purchase_price : `list` of float
        Actual purchase prices.
    purchase_volume : `list` of int
        Purchases volumes.
    purchase_datetime : `list` of `datetime.datetime`
        Purchases datetimes.
    total_purchase_capital : float
        Total purchase capital.
    total_purchase_volume : int
        Total purchase volume.
    target_sale_price : float
        Target sale price.
    stop_loss : float
        Stop loss.
    partial_sale_price : float
        Partial sale price.
    sale_price : `list` of float
        Actual sale prices.
    sale_volume : `list` of int
        Sales volumes.
    sale_datetime : `list` of `datetime.datetime`
        Sales datetimes.
    stop_loss_flag : `list` of `bool`
        Indicator that partial sale price hit triggered sale.
    partial_sale_flag : `list` of `bool`
        Indicator that stop loss triggered sale.
    timeout_flag : `list` of `bool`
        Indicator that max days per operation hit triggered sale.
    total_sale_capital : float
        Total sale capital.
    total_sale_volume : int
        Total sale volume.

    Methods
    ----------
    add_purchase(purchase_price, purchase_volume, purchase_datetime)
        Add purchase execution.
    add_sale(sale_price, sale_volume, sale_datetime, stop_loss_flag=False)
        Add sale execution.
    """
    def __init__(self, ticker):
        self._ticker = ticker
        self._state = State.NOT_STARTED
        self._start_date = None
        self._end_date = None
        self._number_of_orders = 0
        self._target_purchase_price = None
        self._purchase_price = []
        self._purchase_volume = []
        self._purchase_datetime = []
        self._target_sale_price = None
        self._sale_price = []
        self._sale_volume = []
        self._sale_datetime = []
        self._stop_loss_flag = []
        self._partial_sale_flag = []
        self._timeout_flag = []
        self._stop_loss = None
        self._partial_sale_price = None
        self._profit = None
        self._yield = None

    # General properties
    @property
    def ticker(self):
        """str : Ticker name."""
        return self._ticker

    @property
    def state(self):
        """`utils.State` : Operation state (open, close or not started)."""
        return self._state

    @property
    def start_date(self):
        """`datetime.date` : Date of the first purchase."""
        return self._start_date

    @property
    def end_date(self):
        """`datetime.date` : Date of the last sale."""
        return self._end_date

    @property
    def number_of_orders(self):
        """int : Total number of purchase or sale orders."""
        return self._number_of_orders

    @property
    def profit(self):
        """float : Profit after operation close."""
        return self._profit

    @property
    def result_yield(self):
        """float : Yield after operation close."""
        return self._yield

    # Purchase properties
    @property
    def target_purchase_price(self):
        """float : Target purchase price."""
        return self._target_purchase_price

    @target_purchase_price.setter
    def target_purchase_price(self, target_purchase_price):
        if self.state != State.CLOSE:
            self._target_purchase_price = target_purchase_price

    @property
    def purchase_price(self):
        """`list` of float : Actual purchase prices."""
        return self._purchase_price

    @property
    def purchase_volume(self):
        """`list` of int : Purchases volumes."""
        return self._purchase_volume

    @property
    def purchase_datetime(self):
        """`list` of `datetime.datetime` : Purchases datetimes."""
        return self._purchase_datetime

    @property
    def total_purchase_capital(self):
        """float : Total purchase capital."""
        capital = 0.0
        for (purchase, volume) in zip(self._purchase_price, self._purchase_volume):
            capital += purchase * volume
        return round(capital, 2)

    @property
    def total_purchase_volume(self):
        """int : Total purchase volume."""
        total_volume = 0.0
        for volume in self._purchase_volume:
            total_volume += volume
        return total_volume

    # Sale properties
    @property
    def target_sale_price(self):
        """float : Target sale price."""
        return self._target_sale_price

    @target_sale_price.setter
    def target_sale_price(self, target_sale_price):
        if self.state != State.CLOSE:
            self._target_sale_price = target_sale_price

    @property
    def stop_loss(self):
        """float : Stop loss."""
        return self._stop_loss

    @stop_loss.setter
    def stop_loss(self, stop_loss):
        if self.state != State.CLOSE:
            self._stop_loss = stop_loss

    @property
    def partial_sale_price(self):
        """float : Partial sale price."""
        return self._partial_sale_price

    @partial_sale_price.setter
    def partial_sale_price(self, partial_sale_price):
        if self.state != State.CLOSE:
            self._partial_sale_price = partial_sale_price

    @property
    def sale_price(self):
        """`list` of float : Actual sale prices."""
        return self._sale_price

    @property
    def sale_volume(self):
        """`list` of int : Sales volumes."""
        return self._sale_volume

    @property
    def sale_datetime(self):
        """`list` of `datetime.datetime` : Sales datetimes."""
        return self._sale_datetime

    @property
    def partial_sale_flag(self):
        """`list` of `bool` : Indicator that partial sale price hit triggered sale."""
        return self._partial_sale_flag

    @property
    def timeout_flag(self):
        """`list` of `bool` : Indicator that max days per operation hit triggered sale."""
        return self._timeout_flag

    @property
    def stop_loss_flag(self):
        """`list` of `bool` : Indicator that stop loss triggered sale."""
        return self._stop_loss_flag

    @property
    def total_sale_capital(self):
        """float : Total sale capital."""
        capital = 0.0
        for (sale, volume) in zip(self._sale_price, self._sale_volume):
            capital += + sale * volume
        return round(capital, 2)

    @property
    def total_sale_volume(self):
        """int : Total sale volume."""
        total_volume = 0.0
        for volume in self._sale_volume:
            total_volume += + volume
        return total_volume

    def add_purchase(self, purchase_price, purchase_volume, purchase_datetime):
        """
        Add purchase execution.

        Only add if `state` is not close.
        Once add, change `state` to open.

        Args
        ----------
        purchase_price : float
            Purchase price.
        purchase_volume : int
            Purchase volume.
        purchase_datetime : `datetime.datetime`
            Purchase datetime.
        """
        if self.state != State.CLOSE:
            self._purchase_price.append(purchase_price)
            self._purchase_volume.append(purchase_volume)
            self._purchase_datetime.append(purchase_datetime)
            self._number_of_orders += 1

            if self.state == State.NOT_STARTED:
                self._start_date = purchase_datetime
                self._state = State.OPEN

            return True
        return False

    def add_sale(self, sale_price, sale_volume, sale_datetime, stop_loss_flag=False,
        partial_sale_flag=False, timeout_flag=False):
        """
        Add sale execution.

        Only add if `state` is not close.
        Once add, change `state` to open.

        Args
        ----------
        sale_price : float
            Sale price.
        sale_volume : int
            Sale volume.
        sale_datetime : `datetime.datetime`
            Sale datetime.
        stop_loss_flag : bool
            Indicator that stop loss triggered sale.
        partial_sale_flag : bool
            Indicator that partial sale price hit triggered sale.
        timeout_flag : bool
            Indicator that max days per operation hit triggered sale.
        """
        if stop_loss_flag == partial_sale_flag is True:
            logger.error(f"Error arguments \'stop_loss_flag\' and "
                f"\'partial_sale_flag\' can not be True simultaneously.")
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

        if self.state == State.OPEN \
            and self.total_purchase_volume >= self.total_sale_volume + sale_volume:
            self._sale_price.append(sale_price)
            self._sale_volume.append(sale_volume)
            self._sale_datetime.append(sale_datetime)
            self._stop_loss_flag.append(stop_loss_flag)
            self._partial_sale_flag.append(partial_sale_flag)
            self._timeout_flag.append(timeout_flag)
            self._number_of_orders += 1

            if self.total_purchase_volume == self.total_sale_volume:
                self._end_date = sale_datetime
                self._profit = round(self.total_sale_capital - self.total_purchase_capital, 2)
                self._yield = 0.0
                self._state = State.CLOSE

            return True
        return False