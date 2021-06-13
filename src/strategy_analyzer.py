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

        if default_strategy == 'last':
            self._strategy = self._db_strategy_analyzer_model.get_strategy_ids().head(1)

        self._set_strategy_parameters(self._strategy['id'][0])
        self._set_strategy_performance(self._strategy['id'][0])

        self._prepare_app()

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
                    className="header",
                ),
                html.Div(
                    children=[
                        html.H2(
                            children="Strategy: "+self._strategy['name'][0],
                            className="strategy-name"
                        ),
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
                                    ),
                                    margin=dict(l=40, r=0, t=40, b=30),
                                    # className="graph-title"
                                )
                            ),
                            className="graph-box",
                        ),
                        html.Div(
                            children=[
                                html.H3(
                                    children="Statistics"
                                ),
                                dash_table.DataTable (
                                    style_table={
                                        'width': '85%',
                                        'background-color': '#F6F6F6',
                                        'border': '0px',
                                        'margin': 'auto',
                                    },
                                    style_data={
                                        'text-align': 'left',
                                    },
                                    style_cell={
                                        'fontFamily': 'Arial, Helvetica, sans-serif',
                                        'fontSize': '12px',
                                        'boxShadow': '0 0',
                                    },
                                    css=[
                                        {
                                            'selector': 'tr:first-child',
                                            'rule': 'display: none',
                                        }
                                    ],
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
                            className="table-div"
                        )
                    ],
                    className="strategy-div"
                )
            ]
        )

    def run(self):
        self._app.run_server()


if __name__ == "__main__":

    analyzer = StrategyAnalyzer()
    analyzer.run()
