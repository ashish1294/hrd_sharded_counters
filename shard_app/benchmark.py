from itertools import count
import matplotlib.pyplot as mpplt

class GraphPlotter(object):

  @classmethod
  def parse_jmeter_log(cls, filename):
    file_des = open(filename)
    data = file_des.read()
    file_des.close()
    lines = []
    data = data.split('\n')

    for i in data:
      line = i.split(',')
      lines.append(line)

    request_times = []
    for i in lines:
      if i == ['']:
        continue
      i[0] = int(i[0])
      i[1] = int(i[1])
      request_times.append(i)

    def compare(request1, request2):
      return request1[0] - request2[0]

    request_times.sort(compare)

    #return [[i[0] for i in request_times], [i[1] for i in request_times]]
    minutes = []
    time_start = request_times[0][0]
    i = 0
    for time_start in count(request_times[0][0], 60000):
      tmp = []
      while request_times[i][0] < time_start + 60000:
        tmp.append(request_times[i])
        i += 1
        if i >= len(request_times):
          break
      minutes.append(tmp)
      if i >= len(request_times):
        break

    data = []
    for i in minutes:
      number_of_requests = len(i)
      # Cut off for being included
      if number_of_requests < 20:
        continue

      succeded_requests = 0
      total_response_times = 0
      for request in i:
        if request[3].strip() == '200':
          succeded_requests += 1
        total_response_times += request[1]

      data.append([number_of_requests, succeded_requests, total_response_times])

    data.sort(compare)
    final_data = []
    i = 0
    while i < len(data):
      j = i
      tmp = [0, 0, 0]
      while j < len(data):
        if data[i][0] == data[j][0]:
          tmp[0] += data[j][0]
          tmp[1] += data[j][1]
          tmp[2] += data[j][2]
          j += 1
        else:
          break
      tmp[0] = tmp[0] / (j - i)
      tmp[1] = float(tmp[1]) / (j - i)
      tmp[2] = float(tmp[2]) / (j - i)
      i = j
      final_data.append(tmp)

    final_data.sort(compare)
    final_list = [[], [], []]
    for tmp in final_data:
      final_list[0].append(tmp[0])
      final_list[1].append(tmp[1] * 100 / float(tmp[0]))
      final_list[2].append(tmp[2] / float(tmp[0]))

    return final_list

  @classmethod
  def plot_grpahs(cls):
    #Parsing Data from log File
    sharded_test = cls.parse_jmeter_log('sharded_step.csv')
    unsharded_test = cls.parse_jmeter_log('unsharded_step.csv')

    figure = mpplt.figure()
    plt = figure.add_subplot(2, 1, 1)
    plt.set_title("Average Response Time")
    plt.plot(sharded_test[0], sharded_test[2], '-',
             dashes=[4, 4], color='green', label='Sharded Counter')
    plt.plot(unsharded_test[0], unsharded_test[2], color='red',
             label='Unsharded Counter')
    plt.legend()
    plt.set_xlabel('Request Rate / Min')
    plt.set_ylabel('Response Time (ms)')

    plt = figure.add_subplot(2, 1, 2)
    plt.set_title("Transaction Success Rate")
    plt.plot(sharded_test[0], sharded_test[1], '-', dashes=[4, 4],
             color='green', label='Sharded Counter')
    plt.plot(unsharded_test[0], unsharded_test[1], color='red',
             label='Unsharded Counter')
    plt.legend()
    plt.set_xlabel("Request Rate / Min")
    plt.set_ylabel('% of requests succeeded')

    mpplt.show()

    # Only for saving to file
    figure = mpplt.figure()
    plt = figure.add_subplot(1, 1, 1)
    plt.set_title("Average Response Time")
    plt.plot(sharded_test[0], sharded_test[2], '-',
             dashes=[4, 4], color='green', label='Sharded Counter')
    plt.plot(unsharded_test[0], unsharded_test[2], color='red',
             label='Unsharded Counter')
    plt.legend()
    plt.set_xlabel('Request Rate / Min')
    plt.set_ylabel('Response Time (ms)')
    figure.savefig('avg_resp_time.pdf', facecolor='white', edgecolor='black')

    figure.clf()
    plt = figure.add_subplot(1, 1, 1)
    plt.set_title("Transaction Success Rate")
    plt.plot(unsharded_test[0], unsharded_test[1], '-',
             dashes=[4, 4], color='green', label='Sharded Counter')
    plt.plot(sharded_test[0], sharded_test[1], color='red',
             label='Unsharded Counter')
    plt.legend()
    plt.set_xlabel("Request Rate / Min")
    plt.set_ylabel('% of requests succeeded')
    figure.savefig('success_rate.pdf', facecolor='white', edgecolor='black')

GraphPlotter.plot_grpahs()
