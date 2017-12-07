# ParrCorr : Efficient Parallel Methods to Identify Similar Time Series Pairs across Sliding Windows
This page comes with the paper about ParrCorr : Efficient Parallel Methods to Identify Similar Time Series Pairs across Sliding Windows. It gives links to the code and documentation.

# Datasets

We carried out our experiments on synthetic datasets using a random walk data series generator, each data series consisting of 256 points. At each time point the generator draws a random number from a Gaussian distribution N(0,1), then adds the value of the last number to the new number. You can use [Random Walk Time Series Generator](https://github.com/djamelinfo/RandomWalk-TimeSeriesGenerator-On-Spark/wiki) to produce a set of randomWalk datasets.

The real world data represents seismic time series collected from the [IRIS Seismic Data Access repository](http://ds.iris.edu/data/access/).

