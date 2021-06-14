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


class StrategyAnalyzer:

    def __init__(self, default_strategy='last'):
        self._app = None
        self._db_strategy_analyzer_model = DBStrategyAnalyzerModel()

        self._strategy = None
        self._performance = None
        self._tickers_and_dates = None

        if default_strategy == 'last':
            self._strategy = self._db_strategy_analyzer_model.get_strategy_ids().head(1)

        self._set_strategy_parameters(self._strategy['id'][0])
        self._set_strategy_performance(self._strategy['id'][0])
        self._set_strategy_tickers(self._strategy['id'][0])

        self._prepare_app()

        self._set_callbacks()

    def _set_strategy_parameters(self, strategy_id):

        max_percentage_tolerance = 5.0

        statistics_raw = self._db_strategy_analyzer_model.get_strategy_statistics(strategy_id)

        # Transform to percetage if not in
        if statistics_raw['yield'][0] >= 0.0 and statistics_raw['yield'][0] <= max_percentage_tolerance:
            statistics_raw['yield'][0] = round(statistics_raw['yield'][0] * 100, 2)

        if statistics_raw['annualized_yield'][0] >= 0.0 and statistics_raw['annualized_yield'][0] <= max_percentage_tolerance:
            statistics_raw['annualized_yield'][0] = round(statistics_raw['annualized_yield'][0] * 100, 2)

        if statistics_raw['ibov_yield'][0] >= 0.0 and statistics_raw['ibov_yield'][0] <= max_percentage_tolerance:
            statistics_raw['ibov_yield'][0] = round(statistics_raw['ibov_yield'][0] * 100, 2)

        if statistics_raw['annualized_ibov_yield'][0] >= 0.0 and statistics_raw['annualized_ibov_yield'][0] <= max_percentage_tolerance:
            statistics_raw['annualized_ibov_yield'][0] = round(statistics_raw['annualized_ibov_yield'][0] * 100, 2)

        if statistics_raw['avr_tickers_yield'][0] >= 0.0 and statistics_raw['avr_tickers_yield'][0] <= max_percentage_tolerance:
            statistics_raw['avr_tickers_yield'][0] = round(statistics_raw['avr_tickers_yield'][0] * 100, 2)

        if statistics_raw['annualized_avr_tickers_yield'][0] >= 0.0 and statistics_raw['annualized_avr_tickers_yield'][0] <= max_percentage_tolerance:
            statistics_raw['annualized_avr_tickers_yield'][0] = round(statistics_raw['annualized_avr_tickers_yield'][0] * 100, 2)

        if statistics_raw['sharpe_ratio'][0] >= 0.0 and statistics_raw['sharpe_ratio'][0] <= max_percentage_tolerance:
            statistics_raw['sharpe_ratio'][0] = round(statistics_raw['sharpe_ratio'][0] * 100, 2)

        # Round unrounded data
        statistics_raw['volatility'][0] = round(statistics_raw['volatility'][0], 2)

        # Names that will be shown
        statistic_parameters = ['Yield (%)', "Yield (% ann)", 'Volatility (-)', 'Sharpe Ratio (%)', 'Profit (R$)', 'Max Used Capital (R$)', 'IBOVESPA Yield (%)', 'IBOVESPA Yield (% ann)', 'Tickers Average Yield (%)', 'Tickers Average Yield (% ann)']

        statistics_data = [statistics_raw['yield'][0], statistics_raw['annualized_yield'][0], statistics_raw['volatility'][0], statistics_raw['sharpe_ratio'][0], statistics_raw['profit'][0], statistics_raw['max_used_capital'][0], statistics_raw['ibov_yield'][0], statistics_raw['annualized_ibov_yield'][0], statistics_raw['avr_tickers_yield'][0], statistics_raw['annualized_avr_tickers_yield'][0], ]

        self._statistics = pd.DataFrame(data={'parameter': statistic_parameters})
        self._statistics['data'] = statistics_data

    def _set_strategy_performance(self, strategy_id):
        self._performance = self._db_strategy_analyzer_model.get_strategy_performance(strategy_id)

        if self._performance['ibov'][0] != 1.0:
            self._performance['ibov'] = round(self._performance['ibov'] / self._performance['ibov'][0], 4)

        if self._performance['capital'][0] != 1.0:
            self._performance['capital'] = round(self._performance['capital'] / self._performance['capital'][0], 4)

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
                            children="Strategy: "+self._strategy['name'][0],
                            className="strategy-name"
                        ),
                        html.Div(
                            children=[
                                dcc.Graph(
                                    figure=dict(
                                        data=[
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['ibov'],
                                                name='IBOV',
                                                marker=dict(
                                                    color='rgb(37, 37, 37)'
                                                )
                                            ),
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['tickers_average'],
                                                name='Tickers Average',
                                                marker=dict(
                                                    color='rgb(144, 144, 144)'
                                                )
                                            ),
                                            dict(
                                                x=self._performance['day'],
                                                y=self._performance['capital'],
                                                name='Yield',
                                                marker=dict(
                                                    color='rgb(236, 187, 48)'
                                                )
                                            ),
                                        ],
                                        layout=dict(
                                            title='Performance',
                                            showlegend=True,
                                            legend=dict(
                                                x=0,
                                                y=1.0
                                            )
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
                                    id='table',
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
                                                id="ticker-chart", config={"displayModeBar": False},
                                            ),
                                            className="card"
                                        )
                                    ],
                                    className="ticker-chart-wrapper"
                                )
                            ],
                            className="ticker-div"
                        ),
                    ],
                    className="strategy-div"
                )
            ]
        )

    def _update_charts(self, ticker):

        purchase_order_type = 'PURCHASE'
        stop_loss_order_type = 'STOP_LOSS'
        partial_sale_order_type = 'PARTIAL_SALE'
        target_sale_order_type = 'TARGET_SALE'

        purchase_marker_color = 'Green'
        stop_loss_marker_color = 'Red'
        partial_sale_marker_color = 'LightSkyBlue'
        target_sale_marker_color = 'Blue'

        ticker_prices = self._db_strategy_analyzer_model.get_ticker_prices(ticker, pd.to_datetime(self._tickers_and_dates.loc[self._tickers_and_dates['ticker'] == ticker]['initial_date'].values[0]), pd.to_datetime(self._tickers_and_dates.loc[self._tickers_and_dates['ticker'] == ticker]['final_date'].values[0]))

        operations_raw = self._db_strategy_analyzer_model.get_operations(self._strategy['id'][0], ticker)

        operations_data = [
        {
            "name": "Price",
            "x": ticker_prices["day"],
            "y": ticker_prices["close_price"],
            "type": "lines",
            "line": {"color": "orange"},
            "hovertemplate": "R$%{y:.2f}<extra></extra>",
        }]

        only_first_needs_legend_flag = True
        for operation in operations_raw['operation_id'].unique():
            operations_data.append(
                {
                    "name": "Operation",
                    "legendgroup": "group",
                    "x": operations_raw[operations_raw['operation_id'] == operation]['day'].to_list(),
                    "y": operations_raw[operations_raw['operation_id'] == operation]['price'].to_list(),
                    "type": "markers+lines",
                    "line": {"color": "green"},
                    "marker": {'color':[purchase_marker_color if order_type == purchase_order_type else stop_loss_marker_color if order_type == stop_loss_order_type else partial_sale_marker_color if order_type == partial_sale_order_type else target_sale_marker_color if order_type == target_sale_order_type else 'black' for order_type in operations_raw[operations_raw['operation_id'] == operation]['order_type']]},
                    "showlegend": only_first_needs_legend_flag
                }
            )
            only_first_needs_legend_flag = False

        ticker_chart_figure = {
            "data": operations_data,
            "layout": {
                "title": {
                    "text": "Prices and Operations",
                },
                "yaxis": {"tickprefix": "R$"},
                "legend": {"x": 0, "y": 1.0}
            },
        }

        return ticker_chart_figure

    def _set_callbacks(self):
        self._app.callback(
            dash.dependencies.Output('ticker-chart', 'figure'),
            [dash.dependencies.Input('ticker-filter', 'value')]
        )(self._update_charts)

    def run(self):
        self._app.run_server()


if __name__ == "__main__":

    analyzer = StrategyAnalyzer()
    analyzer.run()
