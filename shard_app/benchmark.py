import csv
import matplotlib.pyplot as mpplt
from matplotlib.font_manager import FontProperties

class GraphPlotter(object):

  @classmethod
  def read_csv(cls, filename):

    def process_row(row):
      return [int(row[0]), int(row[1]), int(row[3])]

    # Read Content from file
    with open(filename, 'r') as fstream:
      reader = csv.reader(fstream, quotechar='"')
      data = [process_row(row) for row in reader if len(row) > 0]
    return sorted(data, key=lambda x: x[0])

  @classmethod
  def process_minute(cls, minute):
    zipped = zip(*minute)
    return [len(minute), zipped[2].count(200), sum(zipped[1])]

  @classmethod
  def parse_jmeter_log(cls, filename):
    # Each Request Data
    req_list = cls.read_csv(filename)
    req_list.sort(key=lambda x: x[0])
    total_min = (((req_list[-1][0] - req_list[0][0]) / 120000) + 1)
    minutes = [[] for i in range(total_min)]
    for req in req_list:
      minutes[(req[0] - req_list[0][0]) / 120000].append(req)

    # Each Minute [no_of req, success, total_resp_time]
    data = [cls.process_minute(i) for i in minutes if len(i) >= 20]
    data.sort(key=lambda x: x[0])
    final_data = []
    i = 0

    while i < len(data):
      j = i
      tmp = [0, 0, 0]
      while j < len(data) and data[i][0] == data[j][0]:
        tmp[0] += data[j][0]
        tmp[1] += data[j][1]
        tmp[2] += data[j][2]
        j += 1
      diff = j - i
      tmp[0] = tmp[0] / diff
      tmp[1] = (tmp[1] * 100.0) / (diff * tmp[0])
      tmp[2] = float(tmp[2]) / (diff * tmp[0])
      tmp[0] = tmp[0] / 2
      i = j
      final_data.append(tmp)
    final_list = zip(*final_data)
    return final_list

  @classmethod
  def plot_graphs(cls):
    #Parsing Data from log File
    sharded_test = cls.parse_jmeter_log('load_test/data/sharded.csv')
    unsharded_test = cls.parse_jmeter_log('load_test/data/unsharded.csv')
    memcache_test = cls.parse_jmeter_log('load_test/data/memcache.csv')

    figure = mpplt.figure()
    plt = figure.add_subplot(2, 1, 1)
    plt.set_title("Average Response Time")
    plt.plot(sharded_test[0], sharded_test[2], '-', dashes=[4, 4],
             color='green', label='Sharded Counter')
    plt.plot(unsharded_test[0], unsharded_test[2], color='red',
             label='Unsharded Counter')
    plt.plot(memcache_test[0], memcache_test[2], '.', dashes=[4, 4, 4],
             color='blue', label='Memcache Counter')
    plt.legend()
    plt.set_xlabel('Request Rate / Min')
    plt.set_ylabel('Response Time (ms)')

    plt = figure.add_subplot(2, 1, 2)
    plt.set_title("Transaction Success Rate")
    plt.plot(sharded_test[0], sharded_test[1], '-', dashes=[4, 4],
             color='green', label='Sharded Counter')
    plt.plot(unsharded_test[0], unsharded_test[1], color='red',
             label='Unsharded Counter')
    plt.plot(memcache_test[0], memcache_test[1], '.', dashes=[4, 4, 4],
             color='blue', label='Memcache Counter')
    plt.legend()
    plt.set_xlabel("Request Rate / Min")
    plt.set_ylabel('% of requests succeeded')

    mpplt.show()

    # Only for saving to file
    figure = mpplt.figure()
    plt = figure.add_subplot(1, 1, 1)
    plt.set_title("Average Response Time")
    plt.plot(sharded_test[0], sharded_test[2], '-', dashes=[4, 4],
             color='green', label='Sharded Counter')
    plt.plot(unsharded_test[0], unsharded_test[2], color='red',
             label='Unsharded  Counter')
    plt.plot(memcache_test[0], memcache_test[2], '.', dashes=[4, 4, 4],
             color='blue', label='Memcache Counter')
    plt.set_xlabel('Request Rate / Min')
    plt.set_ylabel('Response Time (ms)')
    box = plt.get_position()
    plt.set_position([box.x0, box.y0 + box.height * 0.1, box.width,
                      box.height * 0.9])
    font = FontProperties()
    font.set_size('small')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
               fancybox=True, shadow=True, ncol=5, prop=font)
    figure.savefig('avg_resp_time.pdf', facecolor='white', edgecolor='black')

    figure.clf()
    plt = figure.add_subplot(1, 1, 1)
    plt.set_ylim([0, 110])
    plt.set_title("Transaction Success Rate")
    plt.plot(unsharded_test[0], unsharded_test[1], '-', dashes=[4, 4],
             color='green', label='Sharded Counter')
    plt.plot(sharded_test[0], sharded_test[1], color='red',
             label='Unsharded Counter')
    plt.plot(memcache_test[0], memcache_test[1], '.', dashes=[4, 4, 4],
             color='blue', label='Memcache Counter')
    plt.set_xlabel("Request Rate / Min")
    plt.set_ylabel('% of requests succeeded')
    box = plt.get_position()
    plt.set_position([box.x0, box.y0 + box.height * 0.1, box.width,
                      box.height * 0.9])
    font = FontProperties()
    font.set_size('small')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1),
               fancybox=True, shadow=True, ncol=5, prop=font)
    figure.savefig('success_rate.pdf', facecolor='white', edgecolor='black')

GraphPlotter.plot_graphs()
