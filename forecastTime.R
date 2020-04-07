source("function.R")
##################
## create forecast
##################
##saveRDS(temp,"data.rds")
##saveRDS(stockGPA,"stockGPA.rds")
##saveRDS(rowStockP,"rowStockP.rds")

##temp <- readRDS("data.rds")
##stockGPA <- readRDS("stockGPA.rds")
##rowStockP <- readRDS("rowStockP.rds")

data <- temp
dataStockGP <- stockGPA

dataRowStock <- rowStockP
dataRowStock[is.na(dataRowStock$rowStock)]$rowStock <-0
dataRowStock[is.na(dataRowStock$rowReserve)]$rowReserve <-0
dataStockGP[is.na(dataStockGP$stockGP)]$stockGP <-0
dataStockGP[is.na(dataStockGP$reserveGP)]$reserveGP <-0

rm(calendar,calendarMaterialIdStoreId,calendarMaterialId,cl,cont)
list = ls()
list[4:length(list)]
rm(list = list[4:length(list)])


################################
## data minus stockGP
################################
data <- data[,.(order = sum(order)),.(storeId,date,orderSkuId,qnt_01,qnt_02,qnt_03,qnt_04,qnt_05,materialId)]
data$date <- ymd(data$date)
names(dataStockGP)[1] <- c("orderSkuId")
setkey(data,storeId,orderSkuId,date)
setkey(dataStockGP,storeId,orderSkuId,date)
data <- merge(data,dataStockGP,all.x = T)
data[is.na(data$stockGP)]$stockGP <-0
data[is.na(data$reserveGP)]$reserveGP <-0
data[,newStock:= ifelse(stockGP - reserveGP >0,stockGP - reserveGP,0)]
data[newStock > 0 & order > 0,newOrder:= ifelse(order > newStock,order - newStock,0) ]
data[,newOrder:=ifelse(is.na(newOrder),order,newOrder)]

data[,sum(orderMaterial)]

data[,order_01:= qnt_01*newOrder]
data[,order_02:= qnt_02*order_01]
data[,order_03:= qnt_03*order_02]
data[,order_04:= qnt_04*order_03]
data[,order_05:= qnt_05*order_04]

data$orderMaterial <- data$order_05 
data$orderMaterial <- ifelse(is.na(data$orderMaterial),data$order_04,data$orderMaterial)
data$orderMaterial <- ifelse(is.na(data$orderMaterial),data$order_03,data$orderMaterial)
data$orderMaterial <- ifelse(is.na(data$orderMaterial),data$order_02,data$orderMaterial)
data$orderMaterial <- ifelse(is.na(data$orderMaterial),data$order_01,data$orderMaterial)


## save information about order - material
fwrite(data[date >= "2019-04-01",.(order = sum(newOrder)),.(month(date),materialId,orderSkuId,storeId)
            ][,.(order = sum(order)),.(month,materialId)],"orderMaterial.csv")
#######################
## alignment time
######################
fwrite(dataRowStock[date == max(dataRowStock$date),],"rowData.csv")
gData <- data[,.(sumMaterial = sum(orderMaterial)),.(date,materialId)]
##fwrite(gData,"gData2.csv")
gData$date <- ymd(gData$date)
minDate <- min(gData$date)
maxDate <- max(gData$date)
calendar <- data.table(seq(minDate,maxDate,by = "day"))
names(calendar) <- c("date")

gData$firstDataWeeks <- gData$date - wday(gData$date,week_start = 1) +1
materialId <- gData[,.(minDate = min(firstDataWeeks),maxDate2 = max(firstDataWeeks)),materialId]
calendarMaterialId <-CJ(calendar$date,materialId$materialId)
names(calendarMaterialId) <- c("date","materialId")
calendarMaterialId <- data.table(calendarMaterialId)

setkey(calendarMaterialId,materialId)
setkey(materialId,materialId)
calendarMaterialId <- merge(calendarMaterialId,materialId)

setkey(calendarMaterialId,date,materialId)
setkey(gData,date,materialId)
gData2 <-merge(calendarMaterialId,gData,all.x = T)
gData2[materialId == "206831a0-63c7-11e2-ad35-ac162d78f2b8" & firstDataWeeks == "2019-08-26"]
gData2[is.na(gData2$sumMaterial)]$sumMaterial <- 0
gData2 <- gData2[date >= minDate & date <= maxDate]

gData2$firstDataWeeks <- gData2$date - wday(gData2$date,week_start = 1) +1
fwrite(gData2,"gData2.csv")
#######add rowStockR
dataRowStock <- dataRowStock[,.(rowStock = sum(rowStock)
                                ,rowReserve = sum(rowReserve)),.(materialId,date)]

setkey(dataRowStock,materialId,date)
setkey(gData2,materialId,date)

gData2 <- merge(gData2,dataRowStock,all.x = T)
gData2[is.na(gData2$rowStock)]$rowStock <-0
## для неполных недель выравниваем продажи в конце и в начале?? а надо ли??
##gData3 <- gData2[,.(sales = sum(sumMaterial),stock = mean(rowStock)),.(materialId,firstDataWeeks)]
##gMaterilId <-gData2[,.(N = .N,maxData = max(firstDataWeeks)),materialId
##                    ][N >=32 & maxData == "2019-03-25"][,.(materialId)]


gData2 <- gData2[,.(sales = sum(sumMaterial),stock = mean(rowStock)),.(materialId,firstDataWeeks)]
data[is.na(data$reserveGP)]$reserveGP <-0

##gData2[firstDataWeeks =="2019-08-26"]$sales <- gData2[firstDataWeeks =="2019-08-26"]$sales * 7/6 
## t <- gData2
##gData2 <- t[firstDataWeeks <= "2019-08-26"]
gMaterilId <-gData2[,.(N = .N,maxData = max(firstDataWeeks)),materialId
                    ][N >= 12][,.(materialId)]

gMaterilId2 <-gData2[!materialId %in% gMaterilId$materialId,.(N = .N,maxData = max(firstDataWeeks)),materialId]
tempDf <- gData2[materialId %in% gMaterilId2$materialId]
fwrite(tempDf,"tempDf.csv")
###################################
## forecast for skuId use function
###################################
##source("function.R")
##no_cores <- detectCores() - 1
##cl <- makeCluster(no_cores)
##registerDoParallel(cl)

##result <- foreach(i = 1:dim(gMaterilId)[1],.packages = c("data.table","forecast")) %dopar% 
##  forecastOneSkuIdTrainTest(gData2,gMaterilId[i])

##stopCluster(cl)
##itogo <- rbindlist(result)
##fwrite(itogo,"itogo.csv")

###################################
## forecast for skuId use function only Train
###################################
source("function.R")
no_cores <- detectCores() - 1
cl <- makeCluster(no_cores)
registerDoParallel(cl)

result <- foreach(i = 1:dim(gMaterilId)[1],.packages = c("data.table","forecast","lubridate")) %dopar% 
  forecastOneSkuIdTrain(gData2,gMaterilId[i])

stopCluster(cl)
itogo <- rbindlist(result)
fwrite(itogo,"itogo.csv")

###################################
## forecast for skuId use function randomForest
###################################
source("function.R")
no_cores <- detectCores() - 1
cl <- makeCluster(no_cores)
registerDoParallel(cl)

result <- foreach(i = 1:(dim(gMaterilId)[1]),.packages = c("data.table","randomForest")) %dopar% 
  forecastOneSkuIdRandomForest(gData2,gMaterilId[i])

stopCluster(cl)
itogo <- rbindlist(result)
fwrite(itogo,"itogo.csv")

##############################
## выравниваем временной ряд (для недели)
##############################
fwrite(gData2,"gdata2.csv")
h <- gData[materialId == "2f816b99-5e7b-11e8-8c7b-2c768a5115e1"]
h <- gData2[materialId == "2f816b99-5e7b-11e8-8c7b-2c768a5115e1"]
ggplot(h,aes(x = firstDataWeeks,y = sales)) + 
  geom_point() + 
  geom_line() +
  geom_line(aes(x = firstDataWeeks,y = stock))

ggplot(h,aes(x = firstDataWeeks,y = sales)) + 
  geom_point() + 
  geom_line()
fwrite(h,"h.csv")
library(forecast)
##145e96ff-8a5a-11e8-ba7d-2c768a5115e1


y <- ts(dt$sales2)
train <- y[1:90]
test <- y[91:100]

##### ses
## train forecast
fc <- ses(train,h=10)
autoplot(fc) +
  autolayer(fitted(fc),series = "Fitted") +
    ylab("sales") + xlab("weeks")
round(accuracy(fc),2)

##### snaive
## train forecast
fc <- stlf(train, h = 10, method = "arima")
fc <- nnetar(train)
fc <- tbats(train)
fc <- auto.arima(train)
autoplot(fcSes) +
  autolayer(fitted(fcSes),series = "Fitted") +
  ylab("sales") + xlab("weeks")
round(accuracy(fc),2)


## test forecast
dtF <- data.table(test)
dtF$ses <- forecast(fc,h=10)$mean
dtF$err <- dtF$ses - dtF$test
dtF$err2 <- dtF$err^2
dtF$per <- 1 - abs(dtF$err)/dtF$test

MAPE <- mean(dtF$per)
MAE <- mean(abs(dtF$err))

### 

autoplot(train, series="Data") +
  autolayer(ma(train,5), series="5-MA") +
  xlab("Year") + ylab("GWh") +
  ggtitle("Annual electricity sales: South Australia") +
  scale_colour_manual(values=c("Data"="grey50","5-MA"="red"),
                      breaks=c("Data","5-MA"))

##############################
## выравниваем временной ряд (для месяца)
##############################
h <- gData
minDate <- min(h$date)
maxDate <- max(h$date)
calendar <- data.table(seq(minDate,maxDate,by = "day"))
names(calendar) <- c("date")

setkey(calendar,date)
setkey(h,date)
dt <-merge(calendar,h)
dt$firstDataMonth <- dt$date - day(dt$date) + 1
dt$firstDataWeeks <- dt$date - wday(dt$date,week_start = 1) +1

dt <- dt[,.(sales = sum(sumMaterial),count = .N),firstDataMonth]


ggplot(dt,aes(x = firstDataMonth,y = sales)) + 
  geom_point()

library(forecast)
y <- ts(dt$sales)

train <- y[6:21]
test <- y[22:24]

## train forecast
fc <- ses(train,h=3)
autoplot(fc) +
  autolayer(fitted(fc),series = "Fitted") +
  ylab("sales") + xlab("weeks")
round(accuracy(fc),2)

## test forecast
dtF <- data.table(test)
dtF$ses <- fc$mean
dtF$err <- dtF$ses - dtF$test
dtF$err2 <- dtF$err^2
dtF$per <- 1 - abs(dtF$err)/dtF$test

MAPE <- mean(dtF$per)
MAE <- mean(abs(dtF$err))

