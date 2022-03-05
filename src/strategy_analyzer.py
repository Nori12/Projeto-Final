import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash_html_components.P import P
import dash_table
import pandas as pd
import plotly.graph_objs as go
import numpy as np
import sys

import constants as c
from db_model import DBStrategyAnalyzerModel
from utils import calculate_yield_annualized

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

MAX_COMMENT_LENGTH = 55

class StrategyAnalyzer:

    def __init__(self, strategy_id=None):
        self._app = None
        self._db_strategy_analyzer_model = DBStrategyAnalyzerModel()

        self._strategy = None
        self._performance = None
        self._tickers_and_dates = None
        self._strategy_id = None
        self._strategy_name = None

        if strategy_id == None:
            # Pick last strategy
            strategies = self._db_strategy_analyzer_model.get_strategy_ids()
            if not strategies.empty:
                self._strategy_id = strategies['id'][0]
            else:
                logger.error("No strategies in database.")
                sys.exit(c.NO_STRATEGY_ERR)
        else:
            self._strategy_id = strategy_id

        self._set_strategy_tickers(self._strategy_id)
        self._set_strategy_parameters(self._strategy_id)
        self._set_strategy_statistics(self._strategy_id)
        self._set_strategy_performance(self._strategy_id)

        self._prepare_app()

        self._set_callbacks()

    def _set_strategy_tickers(self, strategy_id):
        self._tickers_and_dates = \
            self._db_strategy_analyzer_model.get_strategy_tickers(strategy_id)

    def _set_strategy_parameters(self, strategy_id):

        strategy_raw = self._db_strategy_analyzer_model.get_strategy_ids(strategy_id)
        self._strategy_name = strategy_raw['name'][0]

        strategy_raw.drop(['name', 'id', 'alias', 'purchase_margin', 'stop_margin'],
            axis=1, inplace=True)

        strategy_raw['comment'][0] = (strategy_raw['comment'][0][:MAX_COMMENT_LENGTH] + '...') \
            if len(strategy_raw['comment'][0]) > MAX_COMMENT_LENGTH else strategy_raw['comment'][0]

        if 1 >= strategy_raw['risk_capital_product'][0] >= 0:
            strategy_raw['risk_capital_product'][0] = \
                round(strategy_raw['risk_capital_product'][0] * 100, 2)

        if 1 >= strategy_raw['ema_tolerance'][0] >= 0:
            strategy_raw['ema_tolerance'][0] = round(strategy_raw['ema_tolerance'][0] * 100, 2)

        if 1 >= strategy_raw['min_risk'][0] >= 0:
            strategy_raw['min_risk'][0] = round(strategy_raw['min_risk'][0] * 100, 2)

        if 1 >= strategy_raw['max_risk'][0] >= 0:
            strategy_raw['max_risk'][0] = round(strategy_raw['max_risk'][0] * 100, 2)

        strategy_raw['total_capital'][0] = round(strategy_raw['total_capital'][0], 2)
        strategy_raw['number_or_tickers'] = len(self._tickers_and_dates)

        # Names to be shown
        strategy_parameters = ['Comment', 'Total Tickers', 'Start Date', 'End Date',
            'Capital (R$)', 'Risk-Capital Coefficient (%)', 'Gain-Loss Ratio',
            'Minimum Order Volume', 'Minimum Operation Risk (%)', 'Maximum Operation Risk (%)',
            'Partial Sale', 'Stop Loss Type', 'Min Days after Successfull Operation (days)',
            'Min Days after Failure Operation (days)', 'Maximum Days per Operation (days)'
        ]

        strategy_data = [strategy_raw['comment'][0], strategy_raw['number_or_tickers'][0],
            min(self._tickers_and_dates['start_date']).strftime('%d/%m/%Y'),
            max(self._tickers_and_dates['end_date']).strftime('%d/%m/%Y'),
            strategy_raw['total_capital'][0], strategy_raw['risk_capital_product'][0],
            strategy_raw['gain_loss_ratio'][0], strategy_raw['min_order_volume'][0],
            strategy_raw['min_risk'][0], strategy_raw['max_risk'][0],
            str(strategy_raw['partial_sale'][0]).title(), strategy_raw['stop_type'][0].title(),
            strategy_raw['min_days_after_successful_operation'][0],
            strategy_raw['min_days_after_failure_operation'][0],
            strategy_raw['max_days_per_operation'][0]
        ]

        self._strategy = pd.DataFrame(data={'parameter': strategy_parameters})
        self._strategy['data'] = strategy_data

    def _set_strategy_statistics(self, strategy_id):

        statistics_raw = self._db_strategy_analyzer_model.get_strategy_statistics(strategy_id)

        statistics_raw['yield'][0] = round(statistics_raw['yield'][0] * 100, 2)

        statistics_raw['annualized_yield'][0] = \
            round(statistics_raw['annualized_yield'][0] * 100, 2)

        statistics_raw['ibov_yield'][0] = round(statistics_raw['ibov_yield'][0] * 100, 2)

        statistics_raw['annualized_ibov_yield'][0] = \
            round(statistics_raw['annualized_ibov_yield'][0] * 100, 2)

        statistics_raw['avr_tickers_yield'][0] = \
            round(statistics_raw['avr_tickers_yield'][0] * 100, 2)

        statistics_raw['annualized_avr_tickers_yield'][0] = \
            round(statistics_raw['annualized_avr_tickers_yield'][0] * 100, 2)

        statistics_raw['volatility'][0] = round(statistics_raw['volatility'][0] * 100, 2)

        statistics_raw['sharpe_ratio'][0] = round(statistics_raw['sharpe_ratio'][0], 2)

        statistics_raw['max_used_capital'][0] = \
            round(statistics_raw['max_used_capital'][0] * 100, 2)

        if statistics_raw['avg_used_capital'][0] is not None:
            statistics_raw['avg_used_capital'][0] = \
                round(statistics_raw['avg_used_capital'][0] * 100, 2)

        operations_stats = self._db_strategy_analyzer_model.get_operations_statistics(strategy_id)
        total_operations = sum(operations_stats['number'])

        successful_operations = \
            operations_stats[operations_stats['status'] == 'SUCCESS']['number'].squeeze() \
            if not operations_stats[operations_stats['status'] == 'SUCCESS'].empty \
            else 0
        partial_succesful_operations = \
            operations_stats[operations_stats['status'] == 'PARTIAL_SUCCESS']['number'].squeeze() \
            if not operations_stats[operations_stats['status'] == 'PARTIAL_SUCCESS'].empty \
            else 0
        failed_operations = \
            operations_stats[operations_stats['status'] == 'FAILURE']['number'].squeeze() \
            if not operations_stats[operations_stats['status'] == 'FAILURE'].empty \
            else 0
        timeout_operations = \
            operations_stats[operations_stats['status'] == 'TIMEOUT']['number'].squeeze() \
            if not operations_stats[operations_stats['status'] == 'TIMEOUT'].empty \
            else 0
        unfinished_operations = \
            operations_stats[operations_stats['status'] == 'UNFINISHED']['number'].squeeze() \
            if not operations_stats[operations_stats['status'] == 'UNFINISHED'].empty \
            else 0
        # For error identification only
        neither_operations = \
            operations_stats[operations_stats['status'] == 'NEITHER']['number'].squeeze() \
            if not operations_stats[operations_stats['status'] == 'NEITHER'].empty \
            else 0
        if neither_operations > 0:
            logger.error("Unidentified operation status.")
            sys.exit(c.UNIDENTIFIED_OPERATION_STATUS_ERR)

        statistics_raw['volatility'][0] = round(statistics_raw['volatility'][0], 2)

        # CDI statistics
        cdi_df = self._db_strategy_analyzer_model.get_cdi_index(
            min(self._tickers_and_dates['start_date']),
            max(self._tickers_and_dates['end_date']))
        cdi_df['cumulative'] = cdi_df['cumulative'] - 1.0

        cdi_yield = cdi_df['cumulative'].tail(1).squeeze()

        statistics_raw['cdi_yield'] = round(cdi_yield * 100, 2)
        statistics_raw['annualized_cdi_yield'] = round(calculate_yield_annualized(
            cdi_yield, len(cdi_df)) * 100, 4)

        # Active operations statistics
        active_operations = self._db_strategy_analyzer_model.\
            get_strategy_active_operations(self._strategy_id)
        statistics_raw['max_active_operations'] = active_operations['active_operations'].max()
        statistics_raw['avg_active_operations'] = round(np.average(active_operations), 2)
        statistics_raw['std_dev_active_operations'] = round(
            active_operations.describe().loc[['std']].squeeze(), 2)

        # Names to be shown
        statistics_parameters = ['Yield (%)', 'IBOVESPA Yield (%)',
            'Tickers Average Yield (%)', 'CDI Yield (%)', 'Sharpe Ratio (-)',
            'Volatility (%)', 'Maximum Used Capital (%)', 'Average Used Capital (%)',
            'Maximum Active Operations', 'Average Active Operations',
            'Active Operations Standard Deviation', 'Profit (R$)',
            'Total Operations', '---Successful Operations (hit 3:1 target)',
            '---Partial Sale Successfull Operations (hit 1:1 or 2:1 target)',
            '---Failed Operations', '---Timed Out Operations', '---Unfinished Operations',
            'Yield (% ann)', 'IBOVESPA Yield (% ann)', 'Tickers Average Yield (% ann)',
            'CDI Yield (% ann)'
        ]

        statistics_data = [f"{statistics_raw['yield'][0]:.2f}", f"{statistics_raw['ibov_yield'][0]:.2f}",
            f"{statistics_raw['avr_tickers_yield'][0]:.2f}", f"{statistics_raw['cdi_yield'][0]:.2f}",
            statistics_raw['sharpe_ratio'][0], f"{statistics_raw['volatility'][0]:.2f}",
            f"{statistics_raw['max_used_capital'][0]:.2f}", f"{statistics_raw['avg_used_capital'][0]:.2f}",
            statistics_raw['max_active_operations'][0], f"{statistics_raw['avg_active_operations'][0]:.2f}",
            f"{statistics_raw['std_dev_active_operations'][0]:.2f}", statistics_raw['profit'][0],
            total_operations, f"{successful_operations} ({100 * successful_operations/total_operations:.1f}%)",
            f"{partial_succesful_operations} ({100 * partial_succesful_operations/total_operations:.1f}%)",
            f"{failed_operations} ({100 * failed_operations/total_operations:.1f}%)",
            f"{timeout_operations} ({100 * timeout_operations/total_operations:.1f}%)",
            f"{unfinished_operations} ({100 * unfinished_operations/total_operations:.1f}%)",
            f"{statistics_raw['annualized_yield'][0]:.2f}",
            f"{statistics_raw['annualized_ibov_yield'][0]:.2f}",
            f"{statistics_raw['annualized_avr_tickers_yield'][0]:.2f}",
            f"{statistics_raw['annualized_cdi_yield'][0]:.2f}"
        ]

        self._statistics = pd.DataFrame(data={'parameter': statistics_parameters})
        self._statistics['data'] = statistics_data

    def _set_strategy_performance(self, strategy_id):
        self._performance = \
            self._db_strategy_analyzer_model.get_strategy_performance(strategy_id)

        if self._performance['ibov'][0] != 1.0:
            self._performance['ibov'] = round((self._performance['ibov'] /
                self._performance['ibov'][0] - 1) * 100, 2)

        if self._performance['capital'][0] != 1.0:
            self._performance['capital'] = round(
                (self._performance['capital'] / self._performance['capital'][0] - 1) * 100, 2)

        self._performance['capital_in_use'] = round(self._performance['capital_in_use'] * 100, 2)

        if self._performance['tickers_average'][0] == 1.0:
            self._performance['tickers_average'] = round(
                (self._performance['tickers_average'] - 1) * 100, 2)
        elif self._performance['tickers_average'][0] == 0.0:
            self._performance['tickers_average'] = round(
                (self._performance['tickers_average']) * 100, 2)

        cdi_df = self._db_strategy_analyzer_model.get_cdi_index(
            min(self._tickers_and_dates['start_date']),
            max(self._tickers_and_dates['end_date']))
        cdi_df['cumulative'] = cdi_df['cumulative'] - 1.0

        self._performance['cdi'] = round(cdi_df['cumulative'] * 100, 2)

    def _prepare_app(self):

        self._app = dash.Dash(__name__)
        self._app.title = f"Strategy Analytics (ID: {self._strategy_id})"

        self._app.layout = html.Div(
            children=[
                html.Div(
                    children=[
                        html.H1(
                            children="Strategy Analytics", className="header-title"
                        ),
                        html.P(
                            children="Analyze Stock Market swing trade strategies",
                            className="header-description",
                        ),
                    ],
                    className="header-div",
                ),
                html.Div(
                    children=[
                        html.H2(
                            children="Strategy: "+self._strategy_name,
                            className="strategy-name"
                        ),
                        html.Div(
                            children=[
                                dcc.Graph(
                                    figure=dict(
                                        data=[
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['capital'],
                                                name='Strategy Yield',
                                                marker=dict(
                                                    color='rgb(236, 187, 48)'
                                                ),
                                                hovertemplate="%{y:.2f}%"
                                            ),
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['ibov'],
                                                name='IBOV',
                                                marker=dict(
                                                    color='rgb(90, 90, 90)'
                                                ),
                                                hovertemplate="%{y:.2f}%"
                                            ),
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['cdi'],
                                                name='CDI',
                                                marker=dict(
                                                    color='rgb(200, 191, 185)'
                                                ),
                                                hovertemplate="%{y:.2f}%"
                                            ),
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['tickers_average'],
                                                name='Tickers Average',
                                                marker=dict(
                                                    color='rgb(144, 144, 144)'
                                                ),
                                                hovertemplate="%{y:.2f}%",
                                                # visible="legendonly"
                                            ),
                                        ],
                                        layout=dict(
                                            title='Performance',
                                            showlegend=True,
                                            legend=dict(
                                                x=0,
                                                y=1.0
                                            ),
                                            yaxis={"ticksuffix": "%"},
                                            hovermode="x"
                                        )
                                    ),
                                    className="performance-graph"
                                ),
                            ],
                            className="performance-div"
                        ),
                        html.Div(
                            children=[
                                html.H3(
                                    children="Parameters"
                                ),
                               dash_table.DataTable (
                                    style_table={
                                        'width': '70%',
                                        'background-color': '#F6F6F6',
                                        'border': '0px',
                                        'margin': 'auto',
                                    },
                                    style_data={
                                        'whiteSpace': 'normal',
                                        'text-align': 'left',
                                    },
                                    style_cell={
                                        'fontFamily': 'Arial, Helvetica, sans-serif',
                                        'fontSize': '12px',
                                        'boxShadow': '0 0',
                                    },
                                    css=[{
                                        'selector': 'tr:first-child',
                                        'rule': 'display: none',
                                    }],
                                    style_cell_conditional=[{
                                        'if': {'row_index': 'even'},
                                        'backgroundColor': '#F5F5F5'
                                    }] + [{
                                        'if': {'column_id': 'parameter'},
                                        'fontWeight': 'bold'
                                    }],
                                    style_as_list_view=True,
                                    id='parameters-table',
                                    columns=[{"name": i, "id": i} for i in self._strategy.columns],
                                    data=self._strategy.to_dict('records')
                                )
                            ],
                            className="parameters-div"
                        ),
                        html.Div(
                            children=[
                                html.H3(
                                    children="Results and Statistics"
                                ),
                                dash_table.DataTable (
                                    style_table={
                                        'width': '70%',
                                        'background-color': '#F6F6F6',
                                        'border': '0px',
                                        'margin': 'auto',
                                    },
                                    style_data={
                                        'whiteSpace': 'normal',
                                        'text-align': 'left',
                                    },
                                    style_cell={
                                        'fontFamily': 'Arial, Helvetica, sans-serif',
                                        'fontSize': '12px',
                                        'boxShadow': '0 0',
                                    },
                                    css=[{
                                        'selector': 'tr:first-child',
                                        'rule': 'display: none',
                                    }],
                                    style_cell_conditional=[{
                                        'if': {'row_index': 'even'},
                                        'backgroundColor': '#F5F5F5'
                                    }] + [{
                                        'if': {'column_id': 'parameter'},
                                        'fontWeight': 'bold'
                                    }],
                                    style_as_list_view=True,
                                    id='statistics-table',
                                    columns=[{"name": i, "id": i} for i in self._statistics.columns],
                                    data=self._statistics.to_dict('records')
                                )
                            ],
                            className="statistics-div"
                        ),
                        html.Div(
                            children=[
                                dcc.Graph(
                                    figure=dict(
                                        data=[
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['capital_in_use'],
                                                name='Capital in use',
                                                marker=dict(
                                                    color='rgb(236, 187, 48)'
                                                ),
                                                hovertemplate="%{y:.2f}%"
                                            ),
                                        ],
                                        layout=dict(
                                            title='Capital Usage',
                                            showlegend=True,
                                            legend=dict(
                                                x=0,
                                                y=1.0
                                            ),
                                            yaxis={"ticksuffix": "%"},
                                            hovermode="x"
                                        )
                                    ),
                                    className="capital-graph"
                                ),
                            ],
                        ),
                        html.Div(
                            children=[
                                html.H3(
                                    children="Individual Ticker Analysis"
                                ),
                                dcc.Dropdown(
                                    id="ticker-filter",
                                    options=[
                                        {"label": ticker, "value": ticker}
                                        for ticker in np.sort(self._tickers_and_dates.ticker.unique())
                                    ],
                                    value=self._tickers_and_dates['ticker'][0],
                                    clearable=False,
                                    className="ticker-dropdown"
                                ),
                                html.Div(
                                    children=[
                                        html.Div(
                                            children=dcc.Graph(
                                                id="ticker-chart-day", config={"displayModeBar": False},
                                            ),
                                            className="card"
                                        )
                                    ],
                                    className="ticker-chart-day-wrapper"
                                ),
                                html.Div(
                                    children=[
                                        html.Div(
                                            children=dcc.Graph(
                                                id="ticker-chart-week", config={"displayModeBar": False},
                                            ),
                                            className="card"
                                        )
                                    ],
                                    className="ticker-chart-week-wrapper"
                                )
                            ],
                            className="ticker-div"
                        ),
                    ],
                    className="strategy-div"
                )
            ]
        )

    def _update_chart_day(self, ticker):

        purchase_order_type = 'PURCHASE'
        stop_loss_order_type = 'STOP_LOSS'
        partial_sale_order_type = 'PARTIAL_SALE'
        timeout_order_type = 'TIMEOUT'
        target_sale_order_type = 'TARGET_SALE'

        purchase_marker_color = 'Green'
        stop_loss_marker_color = 'Red'
        partial_sale_marker_color = 'LightSkyBlue'
        timeout_marker_color = 'Black'
        target_sale_marker_color = 'Blue'

        ticker_prices = self._db_strategy_analyzer_model.get_ticker_prices_and_features(
            ticker, pd.to_datetime(self._tickers_and_dates.loc[self._tickers_and_dates \
            ['ticker'] == ticker]['start_date'].values[0]), pd.to_datetime(
            self._tickers_and_dates.loc[self._tickers_and_dates['ticker'] == ticker] \
            ['end_date'].values[0]), interval='1d')

        operations_raw = self._db_strategy_analyzer_model.get_operations(self._strategy_id,
            ticker)

        # Ticker prices
        operations_data = [{
            "name": "Price",
            "x": ticker_prices['day'],
            "y": ticker_prices['close_price'],
            "mode": "lines",
            "line": {"color": "orange"},
            "hovertemplate": "R$%{y:.2f}",
            "showlegend": True
        }]

        only_first_needs_legend_flag = True
        for operation in operations_raw['operation_id'].unique():
            operations_data.append(
                {
                    "name": "Operation",
                    "legendgroup": "group",
                    "x": operations_raw[operations_raw['operation_id'] == operation]['day'].to_list(),
                    "y": operations_raw[operations_raw['operation_id'] == operation]['price'].to_list(),
                    "mode": "markers+lines",
                    "line": {"color": "green"},
                    "marker": {"size": 8, "color":[purchase_marker_color
                        if order_type == purchase_order_type
                        else stop_loss_marker_color if order_type == stop_loss_order_type
                        else partial_sale_marker_color if order_type == partial_sale_order_type
                        else target_sale_marker_color if order_type == target_sale_order_type
                        else timeout_marker_color if order_type == timeout_order_type
                        else 'Brown'
                        for order_type in \
                        operations_raw[operations_raw['operation_id'] == operation]['order_type']]},
                    "showlegend": only_first_needs_legend_flag
                }
            )
            only_first_needs_legend_flag = False

        # Ticker peaks
        operations_data.append({
            "name": "Peaks",
            "x": ticker_prices.loc[ticker_prices['peak'] != 0]['day'],
            "y": ticker_prices.loc[ticker_prices['peak'] != 0]['peak'],
            "mode": "markers",
            "showlegend": True,
            "visible": "legendonly",
            "marker": {"color": "black", "symbol": "circle-open", "size": 8, "line": {"width": 2}}
        })

        # Ticker EMA 17
        operations_data.append({
            "name": "EMA 17",
            "x": ticker_prices['day'],
            "y": ticker_prices['ema_17'],
            "mode": "lines",
            "line": {"color": "purple"},
            "showlegend": True,
            "visible": "legendonly"
        })

        # Ticker EMA 17
        operations_data.append({
            "name": "EMA 72",
            "x": ticker_prices['day'],
            "y": ticker_prices['ema_72'],
            "mode": "lines",
            "line": {"color": "yellow"},
            "visible": "legendonly"
        })

        # Ticker Target Purchase Price
        operations_data.append({
            "name": "Buy Price",
            "x": ticker_prices['day'],
            "y": ticker_prices['target_buy_price'],
            "mode": "lines",
            "line": {"color": "lightblue"},
            "showlegend": True,
            "visible": "legendonly"
        })

        # Ticker Stop Loss
        operations_data.append({
            "name": "Stop Loss",
            "x": ticker_prices['day'],
            "y": ticker_prices['stop_loss'],
            "mode": "lines",
            "line": {"color": "darksalmon"},
            "showlegend": True,
            "visible": "legendonly"
        })

        ticker_chart_figure = {
            "data": operations_data,
            "layout": {
                "title": {
                    "text": "Prices, Operations and Features (Day)",
                    "xanchor": "center",
                    "yanchor": "top"
                },
                "yaxis": {"tickprefix": "R$"},
                "legend": {
                    "xanchor": "left",
                    "bgcolor": "rgba(0, 0, 0, 0)",
                },
                "hovermode": "x",
            },
        }

        # Uncomment the following lines to enable green area over uptrend freature
        # # Convert only to add vrect
        # fig = go.Figure(dict(ticker_chart_figure))

        # uptrend_slices = self._get_uptrend_slices(ticker_prices)

        # only_first_needs_legend_flag = True
        # for slice in uptrend_slices:
        #     fig.add_vrect(x0=slice['start'], x1=slice['end'],
        #         fillcolor="green", opacity=0.25, line_width=0)
        #     only_first_needs_legend_flag = False

        # ticker_chart_figure = fig.to_dict()

        return ticker_chart_figure

    def _update_chart_week(self, ticker):

        ticker_prices = self._db_strategy_analyzer_model.get_ticker_prices_and_features(
            ticker, pd.to_datetime(self._tickers_and_dates.loc[self._tickers_and_dates \
            ['ticker'] == ticker]['start_date'].values[0]), pd.to_datetime(
            self._tickers_and_dates.loc[self._tickers_and_dates['ticker'] == ticker] \
            ['end_date'].values[0]), interval='1wk')

        # Ticker prices
        operations_data = [{
            "name": "Price",
            "x": ticker_prices['week'],
            "y": ticker_prices['close_price'],
            "mode": "lines",
            "line": {"color": "orange"},
            "hovertemplate": "R$%{y:.2f}",
            "showlegend": True
        }]

        # Ticker peaks
        operations_data.append({
            "name": "Peaks",
            "x": ticker_prices.loc[ticker_prices['peak'] != 0]['week'],
            "y": ticker_prices.loc[ticker_prices['peak'] != 0]['peak'],
            "mode": "markers",
            "showlegend": True,
            "visible": "legendonly",
            "marker": {"color": "black", "symbol": "circle-open", "size": 8, "line": {"width": 2}}
        })

        # Ticker EMA 17
        operations_data.append({
            "name": "EMA 17",
            "x": ticker_prices['week'],
            "y": ticker_prices['ema_17'],
            "mode": "lines",
            "line": {"color": "purple"},
            "showlegend": True,
            "visible": "legendonly"
        })

        # Ticker EMA 17
        operations_data.append({
            "name": "EMA 72",
            "x": ticker_prices['week'],
            "y": ticker_prices['ema_72'],
            "mode": "lines",
            "line": {"color": "yellow"},
            "visible": "legendonly"
        })

        ticker_chart_figure = {
            "data": operations_data,
            "layout": {
                "title": {
                    "text": "Prices, Operations and Features (Week)",
                    "xanchor": "center",
                    "yanchor": "top"
                },
                "yaxis": {"tickprefix": "R$"},
                "legend": {
                    "xanchor": "left",
                    "bgcolor": "rgba(0, 0, 0, 0)",
                },
                "hovermode": "x",
            },
        }

        return ticker_chart_figure

    # def _get_uptrend_slices(self, dataframe, udt_status_type='default'):

    #     column_name = 'up_down_trend_status_strict' if udt_status_type == 'strict' \
    #         else 'up_down_trend_status'

    #     uptrend_slices = []
    #     last_trend_status = None
    #     last_date = None
    #     current_slice_start_date = None

    #     for index, (_, row) in enumerate(dataframe.iterrows()):
    #         if index == 0:
    #             last_trend_status = row[column_name]
    #             last_date = row['day']
    #         else:
    #             # Enter uptrend interval
    #             if row[column_name] == 1 and last_trend_status != 1:
    #                 current_slice_start_date = row['day']
    #             # Leave uptrend interval
    #             elif row[column_name] != 1 and last_trend_status == 1:
    #                 if current_slice_start_date is not None:
    #                     uptrend_slices.append({"start": current_slice_start_date,
    #                         "end": last_date})
    #                     current_slice_start_date = None
    #                 # If data already starts in uptrend interval
    #                 else:
    #                     uptrend_slices.append({"start": dataframe['day'][0],
    #                         "end": last_date})

    #                 current_slice_start_date = None
    #             # If last slice is unfinished
    #             elif index == len(dataframe) - 1 and row[column_name] == 1 and \
    #                 last_trend_status == 1 and current_slice_start_date is not None:
    #                 uptrend_slices.append({"start": current_slice_start_date,
    #                     "end": row['day']})

    #             last_trend_status = row[column_name]
    #             last_date = row['day']

    #     return uptrend_slices

    def _set_callbacks(self):
        self._app.callback(
            dash.dependencies.Output('ticker-chart-day', 'figure'),
            [dash.dependencies.Input('ticker-filter', 'value')]
        )(self._update_chart_day)
        self._app.callback(
            dash.dependencies.Output('ticker-chart-week', 'figure'),
            [dash.dependencies.Input('ticker-filter', 'value')]
        )(self._update_chart_week)

    def run(self):
        self._app.run_server()

if __name__ == "__main__":

    if len(sys.argv) > 1:
        if int(sys.argv[1]):
            analyzer = StrategyAnalyzer(strategy_id=int(sys.argv[1]))
    else:
        analyzer = StrategyAnalyzer(strategy_id=None)

    analyzer.run()