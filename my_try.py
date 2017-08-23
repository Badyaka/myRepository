# http://easydan.com
# настройка нейрона на Python

import numpy as np
from scipy.optimize import leastsq
from sklearn import datasets
from sklearn.metrics import accuracy_score
from sklearn import cross_validation
from scipy.stats import norm

iris = datasets.load_iris()

def neuron_logit(x, w):
    '''Logit activation function '''
    return 1.0/(1 + np.exp(-np.dot(x, w)))

def neuron_probit(x, w):
    '''Probit activation function'''
    return norm.cdf(np.dot(x, w))


# Change it, if needed
neuron = neuron_probit


X = iris.data
y = iris.target

y[y==0] = 0
y[y!=0] = 1


def ftomin(w, X, y):
    a = [neuron(x_, w) - (1.0 if y_==0 else 0.0) for x_, y_ in zip(X, y)]
    return a

loo = cross_validation.LeavePOut(len(y), 1)
for train_index, test_index in loo:
    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]
    trained_w = leastsq(ftomin, np.random.rand(4), args=(X_train, y_train), maxfev=10**5, full_output=True)
    probabilities = [neuron(x, trained_w[0]) for x in X_test]
    y_pred = [0 if x >= 0.5 else 1 for x in probabilities]
    print ('Accuracy (LOO estimation): ', accuracy_score(y_test, y_pred))
