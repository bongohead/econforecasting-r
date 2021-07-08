# ABOUT
This project contains the code for data scraping, data cleaning, and modeling used to generate forecasts shown on [econforecasting.com](https://econforecasting.com).

---

# RUN NOTES
- Toggle purl = FALSE to run model-nowcasts-backtest.rmd and purl = TRUE to run windows task scheduler
- Run model-asset-contagion.rmd, model-baseling-forecasts.rmd, model-nowcasts-backtest.rmd daily until cron server set up
- Import Data -> Generate Nowcasts (Using History + EOM Forecasts as "Data") -> Generate Forecasts (Use Nowcasts as "Data")

# Dev Notes
- Add: Survey of Consumer Expectations (Has expected inflation at well updated intervals, plus home price change expectations @ one year, 3 year ahead point predictions)
- Consider: Instead of forecasting ahead TDNS1 to TDNS3, forecast ahead individual yields (use tdns only to generate external forecast -> use to forecast intiial baseline, weak, strong) - try later, for now use standard and see if tdns estimates are too off from actual construction
- Add: DFM package for R


# CHANGELOG
## 2021-07-01 [v0.15]
- Added rewrite of model flow: (1) data -> (2) external/qual forecasts; (3) nowcasts; (4) csm
- Removed some old unused files

## 2021-06-24 [v0.14]
- Added better data import system
- Added better data transform system
- Added cleaner EOM aggregation system
- Added automatic cookie scraping from CME site to bypass block
- Added separation of external forecasts from initial forecasts
- Added initial forecasts framework in DB
- Added nowcast SFTP upload
- Added some updates to sentiments model
- Added more imports to WSJ
- Fixed WSJ forecasts since now quarterly (since April 2021)
- Fixed bug with CME scrape (removed cookie set)
- Removed JOLTS new hires from inputs (missing data in 2010)

## 2021-05-25 [v0.13]
- Added importing of releases & release dates for input data series into nowcast models
- Added SQL export of releases & release dates
- Added new shipments, new business applications
- Added releases table to SQL
- Added notes re: purl toggle
- Added code to cleanup old .tex and .pdf files, as well as clean images directory
- Added error logging to task scheduler
- Improved 3-month -> 12-month moving-average inhistorical inflation data
- Fixed bug caused by optimizing DNS yield curve parametrization over MAPE instead of MAE (MAPE was returning Inf values)
- Removed WEI index

## 2021-05-07 [v0.12]
- Added structural basics with impulse response
- Added headers to CME scraping; user-agent now required to access JSON files
- Added task scheduler for Windows automation of scripts
- Added updated project management spreadsheet
- Fixed bug in documentation (xtables)

## 2021-03-19 [v0.11]
- Added backtest code for nowcasts
- Added code for iterating over vintage dates
- Added JOLTS data to model inputs
- Added formatted table for diagnostic factor weights
- Added elastic net regularization to quarterly dynamic factor models
- Added elastic net hyperparameter selection plots
- Added some of the new regularization info to documentation
- Added alpha boundaries to (.5, 1) due to overregularization of some consumption variables 
- Fixed bug related to knitr::purl
- Fixed bug related to SQL insert

## 2021-03-06 [v0.10]
- Added import spreadsheet
- Added GDP subcomponents
- Added deseasonalization information
- Added first monthly interpolation
- Added Kalman filtration code
- Added PCA analysis
- Added dynamic factor models
- Added documentation for Kalman filter
- Added backtransformation functions
- Added many new PCA variable
- Added FRED GET to RETRY
- Added Bai-Ng information criteria for scree plot DFM
- Added Yahoo Finance import
- Added plots for PCA, Kalman filter & smoother, VAR, DFM
- Added in-sample goodness of fit for VAR & DFM
- Added documentation for VAR, DFM, & intro
- Added documentation templates
- Added SQL insert
- Added ability to pull data from particular vintage date
- Added purl code to extract main rmd code and convert it to a function - use for backtesting
- Fixed bug with monthly aggregation
- Fixed bug with specific vintage date pulls from ALFRED database

## 2021-02-13 [v0.09]
- Added mortgage models
- Added misc stationary transform functions for structural modeling
- Added addLags() function
- Added historical data aggregation for SEMs
- Added expected inflation models
- Added prep for Kalman smoothing & interpolation

## 2021-02-09 [v0.08]
- Added monthly aggregation for daily-data forecasts
- Added SOFR web scraping
- Fixed some issues with inconsistent variable names

## 2021-02-03 [v0.05]
- Added dynamic Nelson-Siegel interest rate decomposition & forecasting model
- Added code to allow for SPF pulling for more variables
- Added code to pull historical data of multiple frequencies
- Added SQL inserts to fc tables
- Added SQL insert function
- Improved overall code organization in forecasts.rmd

## 2021-01-29 [v0.04]
- Added historical data scraping code
- Added code for fed funds rate
- Fixed bug in data upload
- Fixed bug in getDataFred()
- Fixed bug in package description compilation

## 2021-01-13 [v0.03]
- Added code for calculating asset contagion index
- Added code for splitting RESET_ALL runs from update runs
- Added code for truncating tables on RESET_ALL runs

## 2021-01-09 [v0.02]
- Added code for remote SQL connection
- Added code for calculation of cross-asset rolling correlation statistics
- Added code for remote SFTP and COPY batch insert of asset-correlation data

## 2021-01-08 [v0.01]
- Added web scraping code for CBO
- Added web scraping code for FRED
- Added web scraping code for WSJ
- Added web scraping code for Philadelphia Fed 

## 2021-01-07 [v0.00]
- Added initial package setup
- Added initial web scraping frameworks etup
