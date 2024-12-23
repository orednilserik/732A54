from math import exp
import matplotlib.pyplot as plt

def kernel_plot(data, h, xlabel, figure_num):
    u = [d / h for d in data]
    res = [exp(-x**2) for x in u]
    plt.figure(figure_num)
    plt.plot(data, res)
    plt.xlabel(xlabel)
    plt.ylabel('kernel')

dist = list(range(0, 5000))
days = list(range(0, 50))
hours = list(range(0, 24))

h_distance = 1000
h_date = 10
h_time = 4

kernel_plot(dist, h_distance, 'distance', 0)
kernel_plot(days, h_date, 'days', 1)
kernel_plot(hours, h_time, 'hours', 2)

times = [24,22,20,18,16,14,12,10,8,6,4]

prods = [13.510553054043463, 14.55853144983272, 16.358096565020862, 17.75028995487805, 18.37995902863405,
       18.300031152459393, 17.6511137558024, 16.443244301074085, 15.048160588889353, 13.786652957936294,12.723081450446688]

sums = [3.011097416665485,3.6492685950797696,4.664154989855703,5.425272421652926,6.384644815671309,6.402708300048972,
         5.882371750729638,4.409092728155342,3.3442929407650848,2.806482342133761,2.4341040672102845]

plt.plot(times, prods)
plt.xlabel('hour')
plt.ylabel('product kernel')

plt.plot(times, sums)
plt.xlabel('hour')
plt.ylabel('summation kernel')