from itertools import count

def parse_jmeter_log(filename):
  fp = open(filename)
  data = fp.read()
  fp.close()
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

  seconds = []
  time_start = request_times[0][0]
  i = 0
  for time_start in count(request_times[0][0], 1000):
    tmp = []
    while request_times[i][0] < time_start + 1000:
      tmp.append(request_times[i])
      i += 1
      if i >= len(request_times):
        break
    seconds.append(tmp)
    if i >= len(request_times):
      break

  data = []
  for i in seconds:
    number_of_requests = len(i)
    if number_of_requests == 0:
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
    i = j
    final_data.append(tmp)

  final_data.sort(compare)
  final_list = [[], [], []]
  for tmp in final_data:
    final_list[0].append(tmp[0])
    final_list[1].append(tmp[1] * 100 / float(tmp[0]))
    final_list[2].append(tmp[2] / float(tmp[0]))

  return final_list