import simulation.load_data as ld
import shared_vars as sv
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import talib

def plot_dataset(dataset: dict[int, dict[int, float]]) -> None:
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    fig, axs = plt.subplots(nrows=4, ncols=2, figsize=(16, 12))
    axs = axs.flatten()

    for weekday in range(7):
        hours = list(range(24))
        values = [dataset[weekday].get(h, 0) for h in hours]

        ax = axs[weekday]
        ax.bar(hours, values, width=0.6)
        ax.set_title(days[weekday])
        ax.set_xlabel('Hour of Day')
        ax.set_ylabel('Accumulated Diff')
        ax.grid(True)
        ax.set_xticks(range(0, 24, 1))

    # Remove unused subplot (if any)
    fig.delaxes(axs[7])

    fig.suptitle('Hourly Accumulated Diff by Day of Week', fontsize=16)
    plt.tight_layout()
    plt.show()

# np.set_printoptions(legacy='1.25')

dataset = {
    0: {},
    1: {},
    2: {},
    3: {},
    4: {},
    5: {},
    6: {}
}

def main() -> None:
    data = ld.load_candles('../MARKET_DATA/_crypto_data/BTCUSDT/BTCUSDT_1h.csv', datetime(2020, 1, 1), datetime(2025, 6, 1))
    
    for i in range(54, len(data)):
        now = data[i-1]
        dt = datetime.fromtimestamp(now[0]/1000)
        hour = dt.hour
        weekday = dt.weekday()
        
        diff = (now[2]-now[3])/now[1]


        if hour not in dataset[weekday]:
            dataset[weekday][hour] = diff
        else:
            dataset[weekday][hour] += diff
    
    print(dataset)
    print(len(data))
    plot_dataset(dataset)
        


if __name__ == "__main__":
    main()
