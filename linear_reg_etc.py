import numpy as np
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
# %matplotlib inline

from sklearn import datasets
# Load the diabetes dataset
diabetes = datasets.load_diabetes()
X = diabetes.data
y = diabetes.target

xx = [i for i in range(X.shape[0])]
f=0
t=40

lr2 = LinearRegression()
lr2.fit(X,y)

y2 = lr2.predict(X)
plt.figure()

plt.plot(xx[f:t], y[f:t], color='r', linewidth=4, label='y')
plt.plot(xx[f:t], y2[f:t], color='b', linewidth=2, label='predicted y')
plt.ylabel('Target label')
plt.xlabel('Line number in dataset')
plt.legend(loc=4)
plt.show()