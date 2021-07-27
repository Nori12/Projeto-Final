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

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

 # TODO: Bug - What if there is no strategy in database?
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
                logger.warning(f"No strategy in database.")
                sys.exit(c.NO_STRATEGY_WAR)
        else:
            self._strategy_id = strategy_id

        self._set_strategy_tickers(self._strategy_id)
        self._set_strategy_parameters(self._strategy_id)
        self._set_strategy_statistics(self._strategy_id)
        self._set_strategy_performance(self._strategy_id)

        self._prepare_app()

        self._set_callbacks()

    def _set_strategy_parameters(self, strategy_id):

        strategy_raw = self._db_strategy_analyzer_model.get_strategy_ids(strategy_id)
        self._strategy_name = strategy_raw['name'][0]

        strategy_raw.drop(['name', 'id', 'alias', 'comment'], axis=1, inplace=True)

        if strategy_raw['risk_capital_product'][0] >= 0 and strategy_raw['risk_capital_product'][0] <= 1:
            strategy_raw['risk_capital_product'][0] = round(strategy_raw['risk_capital_product'][0] * 100, 2)

        strategy_raw['total_capital'][0] = round(strategy_raw['total_capital'][0], 2)

        strategy_raw['number_or_tickers'] = len(self._tickers_and_dates)

        operations_stats = self._db_strategy_analyzer_model.get_operations_statistics(strategy_id)

        # Names that will be shown
        strategy_parameters = ['Start Date', 'End Date', 'Capital (R$)', 'Risk-Capital Coefficient (%)', 'Total Tickers', 'Total Operations', 'Successful Operations', 'Failed Operations', 'Neutral Operations', 'Unfinished Operations']

        total_operations = sum(operations_stats['number'])
        succesful_operations = operations_stats[operations_stats['status'] == 'SUCCESS']['number'].squeeze() if operations_stats[operations_stats['status'] == 'SUCCESS'].empty == False else 0
        failed_operations = operations_stats[operations_stats['status'] == 'FAILURE']['number'].squeeze() if operations_stats[operations_stats['status'] == 'FAILURE'].empty == False else 0
        neutral_operations = operations_stats[operations_stats['status'] == 'NEUTRAL']['number'].squeeze() if operations_stats[operations_stats['status'] == 'NEUTRAL'].empty == False else 0
        open_operations = operations_stats[operations_stats['status'] == 'OPEN']['number'].squeeze() if operations_stats[operations_stats['status'] == 'OPEN'].empty == False else 0

        strategy_data = [min(self._tickers_and_dates['start_date']).strftime('%d/%m/%Y'), max(self._tickers_and_dates['end_date']).strftime('%d/%m/%Y'), strategy_raw['total_capital'][0], strategy_raw['risk_capital_product'][0], strategy_raw['number_or_tickers'][0], total_operations, succesful_operations, failed_operations, neutral_operations, open_operations]

        self._strategy = pd.DataFrame(data={'parameter': strategy_parameters})
        self._strategy['data'] = strategy_data

    def _set_strategy_statistics(self, strategy_id):

        statistics_raw = self._db_strategy_analyzer_model.get_strategy_statistics(strategy_id)

        statistics_raw['yield'][0] = round(statistics_raw['yield'][0] * 100, 2)

        statistics_raw['annualized_yield'][0] = round(statistics_raw['annualized_yield'][0] * 100, 2)

        statistics_raw['ibov_yield'][0] = round(statistics_raw['ibov_yield'][0] * 100, 2)

        statistics_raw['annualized_ibov_yield'][0] = round(statistics_raw['annualized_ibov_yield'][0] * 100, 2)

        statistics_raw['avr_tickers_yield'][0] = round(statistics_raw['avr_tickers_yield'][0] * 100, 2)

        statistics_raw['annualized_avr_tickers_yield'][0] = round(statistics_raw['annualized_avr_tickers_yield'][0] * 100, 2)

        statistics_raw['volatility'][0] = round(statistics_raw['volatility'][0] * 100, 2)

        statistics_raw['sharpe_ratio'][0] = round(statistics_raw['sharpe_ratio'][0], 2)

        # Round unrounded data
        statistics_raw['volatility'][0] = round(statistics_raw['volatility'][0], 2)

        # Names that will be shown
        statistic_parameters = ['Yield (%)', "Yield (% ann)", 'Volatility (%)', 'Sharpe Ratio (-)', 'Profit (R$)', 'Max Used Capital (R$)', 'IBOVESPA Yield (%)', 'IBOVESPA Yield (% ann)', 'Tickers Average Yield (%)', 'Tickers Average Yield (% ann)']

        statistics_data = [statistics_raw['yield'][0], statistics_raw['annualized_yield'][0], statistics_raw['volatility'][0], statistics_raw['sharpe_ratio'][0], statistics_raw['profit'][0], statistics_raw['max_used_capital'][0], statistics_raw['ibov_yield'][0], statistics_raw['annualized_ibov_yield'][0], statistics_raw['avr_tickers_yield'][0], statistics_raw['annualized_avr_tickers_yield'][0], ]

        self._statistics = pd.DataFrame(data={'parameter': statistic_parameters})
        self._statistics['data'] = statistics_data

    def _set_strategy_performance(self, strategy_id):
        self._performance = self._db_strategy_analyzer_model.get_strategy_performance(strategy_id)

        total_capital = self._performance['capital'][0]

        if self._performance['ibov'][0] != 1.0:
            self._performance['ibov'] = round((self._performance['ibov'] /
                self._performance['ibov'][0] - 1) * 100, 2)

        if self._performance['capital'][0] != 1.0:
            self._performance['capital'] = round((self._performance['capital'] / self._performance['capital'][0] - 1) * 100, 2)

        self._performance['capital_in_use'] = round((self._performance['capital_in_use'] / total_capital) * 100, 2)
        self._performance['active_operations'] = self._performance['active_operations']

        if self._performance['tickers_average'][0] == 1.0:
            self._performance['tickers_average'] = round((self._performance['tickers_average'] - 1) * 100, 2)
        elif self._performance['tickers_average'][0] == 0.0:
            self._performance['tickers_average'] = round((self._performance['tickers_average']) * 100, 2)

        cdi_df = self._db_strategy_analyzer_model.get_cdi_index(min(self._tickers_and_dates['start_date']), max(self._tickers_and_dates['end_date']))

        cdi_df['cumulative'] = cdi_df['cumulative'] - 1.0

        self._performance['cdi'] = round(cdi_df['cumulative'] * 100, 2)

    def _set_strategy_tickers(self, strategy_id):
        self._tickers_and_dates = self._db_strategy_analyzer_model.get_strategy_tickers(strategy_id)

    def _prepare_app(self):

        self._app = dash.Dash(__name__)
        self._app.title = "Strategy Analytics: Understand Your Strategies!"

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
                                                name='Yield',
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
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['active_operations'],
                                                name='Active Operations',
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
                                    children="Statistics"
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
                                html.H3(
                                    children="Ticker Individual Analysis"
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
        target_sale_order_type = 'TARGET_SALE'

        purchase_marker_color = 'Green'
        stop_loss_marker_color = 'Red'
        partial_sale_marker_color = 'LightSkyBlue'
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
                    "marker": {"size": 8, "color":[purchase_marker_color if order_type == purchase_order_type else stop_loss_marker_color if order_type == stop_loss_order_type else partial_sale_marker_color if order_type == partial_sale_order_type else target_sale_marker_color if order_type == target_sale_order_type else 'black' for order_type in operations_raw[operations_raw['operation_id'] == operation]['order_type']]},
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
                    "text": "Prices and Operations",
                    "xanchor": "center",
                    "yanchor": "top"
                },
                "yaxis": {"tickprefix": "R$"},
                # "legend": {"x": 0, "y": 1.0},
                "legend": {
                    # "orientation": "h",
                    # "yanchor":"top",
                    "xanchor": "left",
                    "bgcolor": "rgba(0, 0, 0, 0)",
                    # "x": 0,
                    # "y": 1.0
                },
                "hovermode": "x",
            },
        }

        # Convert only to add vrect
        fig = go.Figure(dict(ticker_chart_figure))

        uptrend_slices = self._get_uptrend_slices(ticker_prices)

        only_first_needs_legend_flag = True
        for slice in uptrend_slices:
            fig.add_vrect(x0=slice['start'], x1=slice['end'],
                fillcolor="green", opacity=0.25, line_width=0)
            only_first_needs_legend_flag = False

        ticker_chart_figure = fig.to_dict()

        return ticker_chart_figure

    def _update_chart_week(self, ticker):

        # purchase_order_type = 'PURCHASE'
        # stop_loss_order_type = 'STOP_LOSS'
        # partial_sale_order_type = 'PARTIAL_SALE'
        # target_sale_order_type = 'TARGET_SALE'

        # purchase_marker_color = 'Green'
        # stop_loss_marker_color = 'Red'
        # partial_sale_marker_color = 'LightSkyBlue'
        # target_sale_marker_color = 'Blue'

        ticker_prices = self._db_strategy_analyzer_model.get_ticker_prices_and_features(
            ticker, pd.to_datetime(self._tickers_and_dates.loc[self._tickers_and_dates \
            ['ticker'] == ticker]['start_date'].values[0]), pd.to_datetime(
            self._tickers_and_dates.loc[self._tickers_and_dates['ticker'] == ticker] \
            ['end_date'].values[0]), interval='1wk')

        # operations_raw = self._db_strategy_analyzer_model.get_operations(self._strategy_id,
        #     ticker)

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

        # only_first_needs_legend_flag = True
        # for operation in operations_raw['operation_id'].unique():
        #     operations_data.append(
        #         {
        #             "name": "Operation",
        #             "legendgroup": "group",
        #             "x": operations_raw[operations_raw['operation_id'] == operation]['day'].to_list(),
        #             "y": operations_raw[operations_raw['operation_id'] == operation]['price'].to_list(),
        #             "mode": "markers+lines",
        #             "line": {"color": "green"},
        #             "marker": {"size": 8, "color":[purchase_marker_color if order_type == purchase_order_type else stop_loss_marker_color if order_type == stop_loss_order_type else partial_sale_marker_color if order_type == partial_sale_order_type else target_sale_marker_color if order_type == target_sale_order_type else 'black' for order_type in operations_raw[operations_raw['operation_id'] == operation]['order_type']]},
        #             "showlegend": only_first_needs_legend_flag
        #         }
        #     )
        #     only_first_needs_legend_flag = False

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

        # Ticker Target Purchase Price
        # operations_data.append({
        #     "name": "Buy Price",
        #     "x": ticker_prices['day'],
        #     "y": ticker_prices['target_buy_price'],
        #     "mode": "lines",
        #     "line": {"color": "lightblue"},
        #     "showlegend": True,
        #     "visible": "legendonly"
        # })

        # Ticker Stop Loss
        # operations_data.append({
        #     "name": "Stop Loss",
        #     "x": ticker_prices['day'],
        #     "y": ticker_prices['stop_loss'],
        #     "mode": "lines",
        #     "line": {"color": "darksalmon"},
        #     "showlegend": True,
        #     "visible": "legendonly"
        # })

        ticker_chart_figure = {
            "data": operations_data,
            "layout": {
                "title": {
                    "text": "Prices and Operations",
                    "xanchor": "center",
                    "yanchor": "top"
                },
                "yaxis": {"tickprefix": "R$"},
                # "legend": {"x": 0, "y": 1.0},
                "legend": {
                    # "orientation": "h",
                    # "yanchor":"top",
                    "xanchor": "left",
                    "bgcolor": "rgba(0, 0, 0, 0)",
                    # "x": 0,
                    # "y": 1.0
                },
                "hovermode": "x",
            },
        }

        # Convert only to add vrect
        # fig = go.Figure(dict(ticker_chart_figure))

        # uptrend_slices = self._get_uptrend_slices(ticker_prices)

        # only_first_needs_legend_flag = True
        # for slice in uptrend_slices:
        #     fig.add_vrect(x0=slice['start'], x1=slice['end'],
        #         fillcolor="green", opacity=0.25, line_width=0)
        #     only_first_needs_legend_flag = False

        # ticker_chart_figure = fig.to_dict()

        return ticker_chart_figure

    def _get_uptrend_slices(self, dataframe, udt_status_type='default'):

        column_name = 'up_down_trend_status_strict' if udt_status_type == 'strict' \
            else 'up_down_trend_status'

        uptrend_slices = []
        last_trend_status = None
        last_date = None
        current_slice_start_date = None

        for index, (_, row) in enumerate(dataframe.iterrows()):
            if index == 0:
                last_trend_status = row[column_name]
                last_date = row['day']
            else:
                # Enter uptrend interval
                if row[column_name] == 1 and last_trend_status != 1:
                    current_slice_start_date = row['day']
                # Leave uptrend interval
                elif row[column_name] != 1 and last_trend_status == 1:
                    if current_slice_start_date is not None:
                        uptrend_slices.append({"start": current_slice_start_date,
                            "end": last_date})
                        current_slice_start_date = None
                    # If data already starts in uptrend interval
                    else:
                        uptrend_slices.append({"start": dataframe['day'][0],
                            "end": last_date})

                    current_slice_start_date = None
                # If last slice is unfinished
                elif index == len(dataframe) - 1 and row[column_name] == 1 and \
                    last_trend_status == 1 and current_slice_start_date is not None:
                    uptrend_slices.append({"start": current_slice_start_date,
                        "end": row['day']})

                last_trend_status = row[column_name]
                last_date = row['day']

        return uptrend_slices

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
    analyzer = StrategyAnalyzer(strategy_id=None)
    analyzer.run()
