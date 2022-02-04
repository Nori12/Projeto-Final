import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import stop_loss_optimizer as slo

sys.path.insert(1, '/Users/atcha/Github/Projeto-Final/src')
import constants as c

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

random_colors = ['green', 'blue', 'olive', 'orange', 'red']

class StopLossDataAnalyzer:

    def __init__(self, ticker=None):

        if ticker is None:
            raise Exception('A ticker is needed.')

        self.ticker = ticker

        input_std_file_path = Path(__file__).parent / "out_csv_files" / (ticker + slo.STD_FILE_SUFFIX)
        input_riskmap_file_path = Path(__file__).parent / "out_csv_files" / (ticker + slo.RISKMAP_FILE_SUFFIX)

        sl_df_gen = pd.read_csv(input_std_file_path, sep=",", chunksize=100000)
        self.sl_df = pd.concat((x.query(f"ticker == '{ticker}'") for x in sl_df_gen), ignore_index=True)

        rm_df_gen = pd.read_csv(input_riskmap_file_path, sep=",", chunksize=100000)
        self.rm_df = pd.concat((x.query(f"ticker == '{ticker}'") for x in rm_df_gen), ignore_index=True)

    def show_all_graphs(self):
        fig, ax = plt.subplots(2, 4)

        fig.suptitle(f"{self.ticker} Stop Loss Optimization Analysis")

        fig_width_in_cm = 40
        fig.set_figwidth(fig_width_in_cm / 2.54)
        fig.set_figheight(fig_width_in_cm / 2.54 / 1.78)

        self.plot_success_and_failure(ax[0][0])
        self.plot_failure_reasons(ax[0][1])

        threshold_perc=[0.50, 0.60, 0.70, 0.80, 0.90]

        days_thresholds = self.plot_best_risk_days_histogram(ax[0][2], threshold_perc)
        self.plot_best_risk_values_per_range(ax[0][3], days_thresholds)

        days_thresholds = self.plot_min_risk_days_histogram(ax[1][0], threshold_perc)
        self.plot_min_risk_values_per_range(ax[1][1], days_thresholds)

        days_thresholds = self.plot_max_risk_days_histogram(ax[1][2], threshold_perc)
        self.plot_max_risk_values_per_range(ax[1][3], days_thresholds)

        plt.subplots_adjust(hspace=0.45, wspace=0.3)
        # plt.subplot_tool()

        plt.show()

    def show_risk_map(self):
        fig = plt.figure()
        ax = fig.add_subplot(projection='3d')

        risk_columns = [i for i in self.rm_df.columns.to_list() if i.endswith('_p')]
        risk_ranges = [round(float(i.replace('_p', '').replace('_', '.')), 3)
            for i in self.rm_df.columns.to_list() if i.endswith('_p')]

        x_days_len = c.MAX_DAYS_PER_OPERATION
        y_risks_len = len(risk_columns)

        x_days = []
        y_risks = []

        for idx, col_name in enumerate(risk_columns):
            new_values = self.rm_df.loc[self.rm_df[col_name] > 0, col_name].values.tolist()
            x_days.extend(new_values)
            y_risks.extend([risk_ranges[idx]] * len(new_values))

        hist, xedges, yedges = np.histogram2d(x_days, y_risks,
            bins=(x_days_len-1, y_risks_len-1),
            range=[[1, c.MAX_DAYS_PER_OPERATION], [risk_ranges[0], risk_ranges[-1]]])

        # Construct arrays for the anchor positions
        xpos, ypos = np.meshgrid(xedges[:-1], yedges[:-1], indexing="ij")
        xpos = xpos.ravel()
        ypos = ypos.ravel()
        zpos = 0

        # Construct arrays with the dimensions
        dx = 1
        dy = round(risk_ranges[1] - risk_ranges[0], 4)
        dz = hist.ravel()

        ax.bar3d(xpos, ypos, zpos, dx, dy, dz, zsort='average', shade=True,
            color = 'blue')

        ax.set_title('Successful Operations Histogram')
        ax.set_ylabel('Risk (%)')
        ax.set_xlabel('Days (d)')
        ax.set_zlabel('Count')

        plt.show()

    def plot_success_and_failure(self, axis):

        total = len(self.sl_df)
        suc = round(len(self.sl_df[self.sl_df['success_oper_flag'] == 1]) / total * 100, 1)
        fail = round(len(self.sl_df[self.sl_df['success_oper_flag'] == 0]) / total * 100, 1)

        x = ['Success', 'Failure']
        y = [suc, fail]

        axis.set_title('Success/Failure Chart')
        axis.set_ylabel('Score (%)')
        axis.set_ylim([0, 100])
        axis.bar(x, y, color=['royalblue', 'tomato'])

        for idx, v in enumerate(y):
            axis.text(idx-0.1, v + 5, str(v)+"%", color='black')

    def plot_failure_reasons(self, axis):

        total_fails = len(self.sl_df[self.sl_df['success_oper_flag'] == 0])
        timeout_fail = round(len(self.sl_df[ \
            (self.sl_df['success_oper_flag'] == 0) & \
            (self.sl_df['timeout_flag'] == 1)]) \
            / total_fails * 100, 1)
        std_fail = round(len(self.sl_df[ \
            (self.sl_df['success_oper_flag'] == 0) & \
            (self.sl_df['timeout_flag'] == 0) & \
            (self.sl_df['end_of_interval_flag'] == 0)]) \
            / total_fails * 100, 1)
        end_of_interval_fail = round(len(self.sl_df[
            (self.sl_df['success_oper_flag'] == 0) & \
            (self.sl_df['timeout_flag'] == 0) & \
            (self.sl_df['end_of_interval_flag'] == 1)]) \
            / total_fails * 100, 1)

        labels = ['Normal', 'Time out', 'End of interval']
        values = [std_fail, timeout_fail, end_of_interval_fail]

        axis.pie(values, labels=labels, autopct='%1.1f%%',
                shadow=True, startangle=90, colors=['tomato', 'salmon', 'darkorange'])
        axis.set_title('Failure Reason Chart')
        axis.axis('equal')

    def plot_best_risk_days_histogram(self, axis, threshold_perc=[0.50, 0.60, 0.70, 0.80, 0.90]):

        best_risk_days = self.sl_df.loc[
            (self.sl_df['success_oper_flag'] == 1) & \
            (self.sl_df['best_risk_days'] != 0) & \
            (self.sl_df['end_of_interval_flag'] != 1), ['best_risk_days']]

        n_bins = c.MAX_DAYS_PER_OPERATION - 2
        (n, bins, patches) = axis.hist(best_risk_days['best_risk_days'], bins=n_bins, color='b')

        count_thresholds = [int(p * len(best_risk_days)) for p in threshold_perc]
        cum_sum_of_n = []
        sum = 0
        for value in n:
            sum += value
            cum_sum_of_n.append(sum)

        days_thresholds = []
        for idx, count_threshold in enumerate(count_thresholds):
            bins_idx = cum_sum_of_n.index( next(x for x in cum_sum_of_n if x > count_threshold) )
            axis.axvline((bins[bins_idx] + bins[bins_idx + 1]) / 2, linestyle='dashed', linewidth=1,
                color=random_colors[idx % len(random_colors)],
                label=f">{str(int(threshold_perc[idx] * 100))}%: {int(bins[bins_idx])} days")
            days_thresholds.append(int(bins[bins_idx]))

        axis.set_title('Best Risk Days Histogram')
        axis.set_ylabel('Count')
        axis.set_xlabel('Days')
        axis.legend()

        return days_thresholds

    def plot_best_risk_values_per_range(self, axis, days_thresholds):

        avgs = []
        std_dev = []

        for days in days_thresholds:
            avgs.append(round(self.sl_df.loc[
                (self.sl_df['success_oper_flag'] == 1) & \
                (self.sl_df['best_risk_days'] != 0) & \
                (self.sl_df['end_of_interval_flag'] != 1) & \
                (self.sl_df['best_risk_days'] <= days), ['best_risk']].mean(axis=0).squeeze() * 100, 1))
            std_dev.append(round(self.sl_df.loc[
                (self.sl_df['success_oper_flag'] == 1) & \
                (self.sl_df['best_risk_days'] != 0) & \
                (self.sl_df['end_of_interval_flag'] != 1) & \
                (self.sl_df['best_risk_days'] <= days), ['best_risk']].std(axis=0).squeeze() * 100, 1))

        axis.errorbar(days_thresholds, avgs, std_dev, linestyle='dotted', marker='o',
            ecolor=random_colors[0:len(days_thresholds)], color='blue')
        axis.set_xticks(days_thresholds)
        axis.set_title('Best Risk Mean and Std-dev per Range')
        axis.set_ylabel('Risk (%)')
        axis.set_xlabel('Days Threshold (days)')

    def plot_min_risk_days_histogram(self, axis, threshold_perc=[0.50, 0.60, 0.70, 0.80, 0.90]):

        best_risk_days = self.sl_df.loc[
            (self.sl_df['success_oper_flag'] == 1) & \
            (self.sl_df['min_risk_days'] != 0) & \
            (self.sl_df['end_of_interval_flag'] != 1), ['min_risk_days']]

        n_bins = c.MAX_DAYS_PER_OPERATION - 2
        (n, bins, patches) = axis.hist(best_risk_days['min_risk_days'], bins=n_bins, color='b')

        count_thresholds = [int(p * len(best_risk_days)) for p in threshold_perc]
        cum_sum_of_n = []
        sum = 0
        for value in n:
            sum += value
            cum_sum_of_n.append(sum)

        days_thresholds = []
        for idx, count_threshold in enumerate(count_thresholds):
            bins_idx = cum_sum_of_n.index( next(x for x in cum_sum_of_n if x > count_threshold) )
            axis.axvline((bins[bins_idx] + bins[bins_idx + 1]) / 2, linestyle='dashed', linewidth=1,
                color=random_colors[idx % len(random_colors)],
                label=f">{str(int(threshold_perc[idx] * 100))}%: {int(bins[bins_idx])} days")
            days_thresholds.append(int(bins[bins_idx]))

        axis.set_title('Min Risk Days Histogram')
        axis.set_ylabel('Count')
        axis.set_xlabel('Days')
        axis.legend()

        return days_thresholds

    def plot_min_risk_values_per_range(self, axis, days_thresholds):

        avgs = []
        std_dev = []

        for days in days_thresholds:
            avgs.append(round(self.sl_df.loc[
                (self.sl_df['success_oper_flag'] == 1) & \
                (self.sl_df['min_risk_days'] != 0) & \
                (self.sl_df['end_of_interval_flag'] != 1) & \
                (self.sl_df['min_risk_days'] <= days), ['min_risk']].mean(axis=0).squeeze() * 100, 1))
            std_dev.append(round(self.sl_df.loc[
                (self.sl_df['success_oper_flag'] == 1) & \
                (self.sl_df['min_risk_days'] != 0) & \
                (self.sl_df['end_of_interval_flag'] != 1) & \
                (self.sl_df['min_risk_days'] <= days), ['min_risk']].std(axis=0).squeeze() * 100, 1))

        axis.errorbar(days_thresholds, avgs, std_dev, linestyle='dotted', marker='o',
            ecolor=random_colors[0:len(days_thresholds)], color='blue')
        axis.set_xticks(days_thresholds)
        axis.set_title('Min Risk Mean and Std-dev per Range')
        axis.set_ylabel('Risk (%)')
        axis.set_xlabel('Days Threshold (days)')

    def plot_max_risk_days_histogram(self, axis, threshold_perc=[0.50, 0.60, 0.70, 0.80, 0.90]):

        best_risk_days = self.sl_df.loc[
            (self.sl_df['success_oper_flag'] == 1) & \
            (self.sl_df['max_risk_days'] != 0) & \
            (self.sl_df['end_of_interval_flag'] != 1), ['max_risk_days']]

        n_bins = c.MAX_DAYS_PER_OPERATION - 2
        (n, bins, patches) = axis.hist(best_risk_days['max_risk_days'], bins=n_bins, color='b')

        count_thresholds = [int(p * len(best_risk_days)) for p in threshold_perc]
        cum_sum_of_n = []
        sum = 0
        for value in n:
            sum += value
            cum_sum_of_n.append(sum)

        days_thresholds = []
        for idx, count_threshold in enumerate(count_thresholds):
            bins_idx = cum_sum_of_n.index( next(x for x in cum_sum_of_n if x > count_threshold) )
            axis.axvline((bins[bins_idx] + bins[bins_idx + 1]) / 2, linestyle='dashed', linewidth=1,
                color=random_colors[idx % len(random_colors)],
                label=f">{str(int(threshold_perc[idx] * 100))}%: {int(bins[bins_idx])} days")
            days_thresholds.append(int(bins[bins_idx]))

        axis.set_title('Max Risk Days Histogram')
        axis.set_ylabel('Count')
        axis.set_xlabel('Days')
        axis.legend()

        return days_thresholds

    def plot_max_risk_values_per_range(self, axis, days_thresholds):

        avgs = []
        std_dev = []

        for days in days_thresholds:
            avgs.append(round(self.sl_df.loc[
                (self.sl_df['success_oper_flag'] == 1) & \
                (self.sl_df['max_risk_days'] != 0) & \
                (self.sl_df['end_of_interval_flag'] != 1) & \
                (self.sl_df['max_risk_days'] <= days), ['max_risk']].mean(axis=0).squeeze() * 100, 1))
            std_dev.append(round(self.sl_df.loc[
                (self.sl_df['success_oper_flag'] == 1) & \
                (self.sl_df['max_risk_days'] != 0) & \
                (self.sl_df['end_of_interval_flag'] != 1) & \
                (self.sl_df['max_risk_days'] <= days), ['max_risk']].std(axis=0).squeeze() * 100, 1))

        axis.errorbar(days_thresholds, avgs, std_dev, linestyle='dotted', marker='o',
            ecolor=random_colors[0:len(days_thresholds)], color='blue')
        axis.set_xticks(days_thresholds)
        axis.set_title('Max Risk Mean and Std-dev per Range')
        axis.set_ylabel('Risk (%)')
        axis.set_xlabel('Days Threshold (days)')

if __name__ == '__main__':
    sl_analyzer = StopLossDataAnalyzer(ticker='MGLU3')
    sl_analyzer.show_all_graphs()
    sl_analyzer.show_risk_map()
