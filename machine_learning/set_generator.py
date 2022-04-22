import sys
from pathlib import Path
import time
from tqdm import tqdm
import psutil
import argparse
from multiprocessing import Pool
import pandas as pd

sys.path.insert(1, str(Path(__file__).parent.parent/'src'))
import ml_constants as mlc
import config_reader as cr
from ticker_dataset_generator import TickerDatasetGenerator

pbar = None

def run_ticker_dataset(ticker, start_date, end_date, buy_type='current_day_open_price',
    gain_loss_ratio=3, peaks_pairs_number=2, risk_option='fixed', fixed_risk=0.03,
    start_range_risk=0.01, step_range_risk=0.002, end_range_risk=0.12,
    max_days_per_operation=45, spearman_correlations=(3, 17, 72), datasets_dir=None,
    add_ref_price=False):

    ticker_ds_gen = TickerDatasetGenerator(ticker=ticker, start_date=start_date,
        end_date=end_date, buy_type=buy_type, gain_loss_ratio=gain_loss_ratio,
        peaks_pairs_number=peaks_pairs_number, risk_option=risk_option,
        fixed_risk=fixed_risk, start_range_risk=start_range_risk,
        step_range_risk=step_range_risk, end_range_risk=end_range_risk,
        max_days_per_operation=max_days_per_operation,
        spearman_correlations=spearman_correlations, dataset_dir=datasets_dir)

    ticker_ds_gen.generate_dataset(add_ref_price=add_ref_price)


if __name__ == '__main__':

    print('Set Generator started.')

    # Parse args
    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--pools", type=int,
        help="Number of worker processes to run code in parallel.")
    parser.add_argument("-s", "--start-on-ticker", type=int, default=1,
        help="Number of ticker to start. Default is 1.")
    parser.add_argument("-t", "--max-tickers", type=int, default=None,
        help="Maximum number of tickers to evaluate.")
    parser.add_argument("-e", "--end-on-ticker", type=int, default=None,
        help="Number of ticker to end.")
    parser.add_argument("-b", "--buy-type", default='current_day_open_price',
        choices=['current_day_open_price', 'last_day_close_price'],
        help="Buy type.")
    parser.add_argument("-g", "--gain-loss-ratio", type=int, default=3,
        help="Gain loss ratio. Default is 3.")
    parser.add_argument("-n", "--peaks-pairs-number", type=int, default=2,
        help="Number of past max and min peaks pairs. '2' means '2' last min " \
        "peaks and '2' last max peaks. Default is 2.")
    parser.add_argument("-r", "--risk-option", default='range',
        choices=['fixed', 'range'],
        help="Risk option type. 'fixed' means only 'fixed_risk' will be evaluated " \
        "per day. 'range' means ['start_range_risk', 'end_range_risk'] in steps " \
        "of 'step_range_risk'.")
    parser.add_argument("-f", "--fixed-risk", type=float, default=0.03,
        help="Fixed risk value. Only used if 'risk-option' is set to 'fixed'. " \
        "Default is 0.03.")
    parser.add_argument("-x", "--start-range-risk", type=float, default=0.01,
        help="Start range risk value. Only used if 'risk-option' is set to 'range'. " \
        "Default is 0.01.")
    parser.add_argument("-y", "--step-range-risk", type=float, default=0.002,
        help="Step range risk value. Only used if 'risk-option' is set to 'range'. " \
        "Default is 0.002.")
    parser.add_argument("-z", "--end-range-risk", type=float, default=0.12,
        help="Step range risk value. Only used if 'risk-option' is set to 'range'. " \
        "Default is 0.002.")
    parser.add_argument("-m", "--max-op-days", type=int, default=45,
        help="Max days per operation. If the duration exceeds this value, it is " \
        "considered a failure operation. Default is 45.")
    parser.add_argument("-d", "--datasets-dir", default=None,
        help="Datasets output file. " \
        "Default is 0.002.")
    parser.add_argument("-c", "--spearman-correlations",
        default="[3, 5, 10, 15, 17, 20, 30, 40, 50, 70, 72]",
        help="List of last n prices to calculate spearman correlation. " \
        "Default is '[3, 5, 10, 15, 17, 20, 30, 40, 50, 70, 72]'.")
    parser.add_argument("-a", "--add-ref-price", type=bool, default=True,
        help="Add reference price in each row of dataset. Default is 'True'.")

    args = parser.parse_args()

    # ************************ Check 'pools' argument **************************
    max_pools = psutil.cpu_count(logical=False)

    if args.pools is not None:
        max_pools = args.pools

    print(f"Using maximum of {max_pools} worker processes.")
    # **************************************************************************

    # **** Check 'start_on_ticker', 'max_tickers', 'end_on_ticker' arguments ***
    cfg_path = Path(__file__).parent / 'config.json'
    config_reader = cr.ConfigReader(config_file_path=cfg_path)
    tickers_and_dates = config_reader.tickers_and_dates

    if len(tickers_and_dates) < 1:
        print("Config file must have at least one ticker to evaluate.")
        sys.exit()

    start_on_ticker = 1
    max_tickers = None
    end_on_ticker = len(tickers_and_dates)

    if args.start_on_ticker is not None:
        if args.start_on_ticker < 1:
            print("'start_on_ticker' minimum value is 1.")
            sys.exit()
        elif args.start_on_ticker > len(tickers_and_dates):
            print(f"'start_on_ticker' maximum value is {len(tickers_and_dates)} " \
                f"due to Config file.")
            sys.exit()
        else:
            start_on_ticker = args.start_on_ticker

    if args.max_tickers is not None:
        if args.max_tickers < 1:
            print("'max_tickers' minimum value is 1.")
            sys.exit()
        else:
            max_tickers = args.max_tickers
            end_on_ticker = start_on_ticker + min(max_tickers - 1, len(tickers_and_dates))

    if args.end_on_ticker is not None:
        if args.end_on_ticker < 1:
            print("'end_on_ticker' minimum value is 1.")
            sys.exit()
        elif args.end_on_ticker < start_on_ticker:
            print("'end_on_ticker' must be greater than or equal to " \
                "'start_on_ticker'.")
            sys.exit()
        elif args.end_on_ticker > len(tickers_and_dates):
            print(f"'end_on_ticker' maximum value is {len(tickers_and_dates)} " \
                f"due to Config file.")
            sys.exit()

        if max_tickers is not None:
            end_on_ticker = min(args.end_on_ticker, start_on_ticker + max_tickers - 1)
        else:
            end_on_ticker = args.end_on_ticker

    # Filter tickers_and_dates to only get tickers to evaluate
    tickers = {}
    for idx, (ticker, dates) in enumerate(tickers_and_dates.items()):
        if idx + 1 >= start_on_ticker and idx + 1 <= end_on_ticker:
            tickers[ticker] = dates.copy()
    # **************************************************************************

    # *********************** Check 'buy_type' argument ************************
    buy_type = args.buy_type
    # **************************************************************************

    # ******************** Check 'gain_loss_ratio' argument ********************
    gain_loss_ratio = args.gain_loss_ratio
    # **************************************************************************

    # ****************** Check 'peaks_pairs_number' argument *******************
    peaks_pairs_number = args.peaks_pairs_number
    # **************************************************************************

    # ********************** Check 'risk_option' argument **********************
    risk_option = args.risk_option
    # **************************************************************************

    # ********************** Check 'fixed_risk' argument ***********************
    fixed_risk = args.fixed_risk
    # **************************************************************************

    # * Check 'start_range_risk', 'step_range_risk', 'end_range_risk' arguments*

    start_range_risk = args.start_range_risk
    step_range_risk = args.step_range_risk
    end_range_risk = args.end_range_risk

    if end_range_risk < start_range_risk:
        print("'end-range-risk' must be greater than 'start-range-risk'.")
        sys.exit()
    # **************************************************************************

    # **************** Check 'max_op_days' argument *****************
    max_days_per_operation = args.max_op_days
    # **************************************************************************

    # ********************* Check 'datasets_dir' argument **********************
    datasets_dir = None
    if args.datasets_dir is not None:
        datasets_dir = Path(args.datasets_dir)
    # **************************************************************************

    # ***************** Check 'spearman_correlations' argument *****************

    arg = args.spearman_correlations.replace('[', '')
    arg = arg.replace(']', '')
    arg = arg.strip()

    raw_values = arg.split(',')
    spearman_correlations = []

    for raw_value in raw_values:
        spearman_correlations.append(int(raw_value))

    spearman_correlations = tuple(spearman_correlations)
    # **************************************************************************

    # ********************* Check 'add_ref_price' argument *********************
    add_ref_price = args.add_ref_price
    # **************************************************************************

    pbar = tqdm(total=len(tickers))
    start = time.perf_counter()

    with Pool(max_pools) as pool:
        for ticker, dates in tickers.items():

            pool.apply_async(run_ticker_dataset, (ticker, pd.Timestamp(dates['start_date']),
                pd.Timestamp(dates['end_date']), buy_type, gain_loss_ratio, peaks_pairs_number,
                risk_option, fixed_risk, start_range_risk, step_range_risk, end_range_risk,
                max_days_per_operation, spearman_correlations, datasets_dir, add_ref_price),
                callback=lambda x: pbar.update())

            # TODO: Remove mock
            break

        pool.close()
        pool.join()

    finish = time.perf_counter()
    pbar.close()

    print(f"Finished in {int((finish - start) // 60)}min " \
        f"{int((finish - start) % 60)}s.")
