import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
xk = np.arange(7)
pk = (0.01, 0.01, 0.01, 0.01, 0.01, 0.94, 0.01)
custm = stats.rv_discrete(name='custm', values=(xk, pk))
h = plt.plot(xk, custm.pmf(xk))

r = custm.rvs(size=1)
print r

pk = (0.01, 0.01, 0.01, 0.01, 0.01, 0.01, 0.94)
custm = stats.rv_discrete(name='custm', values=(xk, pk))
h = plt.plot(xk, custm.pmf(xk))

r = custm.rvs(size=1)
print r
