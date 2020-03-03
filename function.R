library(data.table)
library(doParallel)
library(forecast)
library(randomForest)

##h<- gData2
##skuId <- "206831bd-63c7-11e2-ad35-ac162d78f2b8"
## skuId <- gMaterilId[1]

forecastOneSkuIdTrainTest <- function(h,skuId,lenghtForecast = 12) {
  
  setkey(h,materialId)
  h <- h[skuId]
  ## minus negative forecast
  h[sales <=0]$sales <- 0
  meanTrain <- mean(h$sales)
  ##h[sales == 0 & stock <=1]$sales <- meanTrain
  ##h$sales <- log(h$sales + 1)
  ## prepare Data for tain and test
  h <- h[order(firstDataWeeks)]
  y <- ts(h$sales)
  
  trainLength <- length(y) - lenghtForecast
  
  train <- y[1:trainLength]
  test  <- y[(trainLength + 1):length(y)]
  testTrain <- y[(trainLength - lenghtForecast + 1):trainLength]
  
  ## create mean for 0
  ##train <- data.table(train)
  ##meanTrain <- mean(train$train)
  ##train[train == 0]$train <- meanTrain
  ##train <- ts(train$train)
  
  ##ses training
  fcSes <- ses(train,h=lenghtForecast)
  resultSes <- predict(fcSes,lenghtForecast)$mean
  fcSesMAE <- round(accuracy(fcSes),12)[3]
  ##fcSesMAPE <- fcSes$residuals[(trainLength - lenghtForecast + 1):trainLength]
  
  ## mean forecast
  ##fcMean <- mean(train)
  ##resultMean <- rep(fcMean,lenghtForecast)
  
  ##snaiveFun
  fcSnaive <- snaive(train,lenghtForecast)
  resultSnaive <- predict(fcSnaive,lenghtForecast)$mean
  fcSnaiveMAE <- round(accuracy(fcSnaive),12)[3]
  
  ##holt
  fcHolt <- holt(train,lenghtForecast)
  resultHolt <- predict(fcHolt,lenghtForecast)$mean
  fcHoltMAE <- round(accuracy(fcHolt),12)[3]
  
  ##tbats
  fcTbats <- tbats(train)
  resultTbats <- forecast(fcTbats,lenghtForecast)$mean
  fcTbatsMAE <- round(accuracy(fcTbats),12)[3]
  
  ##arima
  fcArima <- auto.arima(train)
  resultArima <- forecast(fcArima,lenghtForecast)$mean
  fcArimaMAE <- round(accuracy(fcArima),12)[3]
  
  ##croston
  fcCroston <- croston(train,lenghtForecast,alpha = 0.2)
  resultCroston <- predict(fcCroston,lenghtForecast)$mean
  fcCrostonMAE <- round(accuracy(fcCroston),12)[3]
  
  ##make data.table ses
  ##dtF <- h[((trainLength+1):length(y)),.(firstDataWeeks,sales)]
  ##names(dtF)[2] <- c("test")
  ##dtF$name <- "ses"
  ##dtF$MAE <- fcSesMAE
  ##dtF$forecast <- resultSes
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  ##dtItogo <- dtF
  
  ##make data.table snaiveFun
  ##dtF <- data.table(test)
  ##dtF$name <- "snaive"
  ##dtF$MAE <- fcSnaiveMAE
  ##dtF$forecast <- resultSnaive
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  ##dtItogo <- rbind(dtItogo,dtF)
  ##make data.table holt
  ##dtF <- data.table(test)
  ##dtF$name <- "holt"
  ##dtF$MAE <- fcHoltMAE
  ##dtF$forecast <- resultHolt
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  ##dtItogo <- rbind(dtItogo,dtF)
  ##make data.table tbats
  dtF <- h[((trainLength+1):length(y)),.(firstDataWeeks,sales)]
  names(dtF)[2] <- c("test")
  dtF$name <- "tbats"
  dtF$MAE <- fcTbatsMAE
  dtF$forecast <- resultTbats
  dtF$err <- dtF$forecast - dtF$test
  dtF$err2 <- dtF$err^2
  dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  dtItogo <- dtF
  ##dtItogo <- rbind(dtItogo,dtF)
  
  ##make data.table arima
  ##dtF <- h[((trainLength+1):length(y)),.(firstDataWeeks,sales)]
  ##names(dtF)[2] <- c("test")
  ##dtF$name <- "arima"
  ##dtF$MAE <- fcArimaMAE
  ##dtF$forecast <- resultArima
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  

  ##dtItogo <- rbind(dtItogo,dtF)
  
  ##make data.table croston
  ##dtF <- data.table(test)
  ##dtF$name <- "croston"
  ##dtF$MAE <- fcCrostonMAE
  ##dtF$forecast <- resultCroston
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  ##dtItogo <- rbind(dtItogo,dtF)
  
  
  dtItogo$skuId <- skuId
  dtItogo$N <- length(y)
  dtItogo$minMAE <- min(dtItogo$MAE)
  ##dtItogo$forecast <- exp(dtItogo$forecast) - 1
  ##dtItogo$test <- exp(dtItogo$test) - 1
  
  dtItogo <- dtItogo[minMAE == MAE]
  return(dtItogo)
}

forecastOneSkuIdTrain <- function(h,skuId,lenghtForecast = 12) {
  
  setkey(h,materialId)
  h <- h[skuId]
  ## minus negative forecast
  h[sales <=0]$sales <- 0
  meanTrain <- mean(h$sales)
  ##h[sales == 0 & stock <=1]$sales <- meanTrain
  ##h$sales <- log(h$sales + 1)
  ## prepare Data for tain and test
  ##h <- h[firstDataWeeks < "2019-04-01"]
  h <- h[order(firstDataWeeks)]
  y <- ts(h$sales)
  
  
  
  train <- y
  test  <- c(1:lenghtForecast)
  
  
  ## create mean for 0
  ##train <- data.table(train)
  ##meanTrain <- mean(train$train)
  ##train[train == 0]$train <- meanTrain
  ##train <- ts(train$train)
  
  ##ses training
  ##fcSes <- ses(train,h=lenghtForecast)
  ##resultSes <- predict(fcSes,lenghtForecast)$mean
  ##fcSesMAE <- round(accuracy(fcSes),12)[3]
  ##fcSesMAPE <- fcSes$residuals[(trainLength - lenghtForecast + 1):trainLength]
  
  ## mean forecast
  ##fcMean <- mean(train)
  ##resultMean <- rep(fcMean,lenghtForecast)
  
  ##snaiveFun
  ##fcSnaive <- snaive(train,lenghtForecast)
  ##resultSnaive <- predict(fcSnaive,lenghtForecast)$mean
  ##fcSnaiveMAE <- round(accuracy(fcSnaive),12)[3]
  
  ##holt
  ##fcHolt <- holt(train,lenghtForecast)
  ##resultHolt <- predict(fcHolt,lenghtForecast)$mean
  ##fcHoltMAE <- round(accuracy(fcHolt),12)[3]
  
  ##tbats
  fcTbats <- tbats(train)
  resultTbats <- forecast(fcTbats,lenghtForecast)$mean
  fcTbatsMAE <- round(accuracy(fcTbats),12)[3]
  
  ##arima
  ##fcArima <- auto.arima(train)
  ##resultArima <- forecast(fcArima,lenghtForecast)$mean
  ##fcArimaMAE <- round(accuracy(fcArima),12)[3]
  
  ##croston
  ##fcCroston <- croston(train,lenghtForecast,alpha = 0.2)
  ##resultCroston <- predict(fcCroston,lenghtForecast)$mean
  ##fcCrostonMAE <- round(accuracy(fcCroston),12)[3]
  
  ##make data.table ses
  ##dtF <- h[((trainLength+1):length(y)),.(firstDataWeeks,sales)]
  ##names(dtF)[2] <- c("test")
  ##dtF$name <- "ses"
  ##dtF$MAE <- fcSesMAE
  ##dtF$forecast <- resultSes
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  ##dtItogo <- dtF
  
  ##make data.table snaiveFun
  ##dtF <- data.table(test)
  ##dtF$name <- "snaive"
  ##dtF$MAE <- fcSnaiveMAE
  ##dtF$forecast <- resultSnaive
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  ##dtItogo <- rbind(dtItogo,dtF)
  ##make data.table holt
  ##dtF <- data.table(test)
  ##dtF$name <- "holt"
  ##dtF$MAE <- fcHoltMAE
  ##dtF$forecast <- resultHolt
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  ##dtItogo <- rbind(dtItogo,dtF)
  ##make data.table tbats
  minDate <- max(h$firstDataWeeks + 7)
  calendar <- data.table(seq(minDate,minDate + 1800,by = "day"))
  names(calendar) <- c("date")
  calendar$firstDataWeeks <- calendar$date - wday(calendar$date,week_start = 1) +1
  calendar <- calendar[,.N,firstDataWeeks]
  
  dtF <- calendar[1:lenghtForecast,.(firstDataWeeks,N)]
  names(dtF)[2] <- c("test")
  dtF$name <- "tbats"
  dtF$MAE <- fcTbatsMAE
  dtF$forecast <- resultTbats
  dtF$err <- dtF$forecast - dtF$test
  dtF$err2 <- dtF$err^2
  dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  dtItogo <- dtF
  ##dtItogo <- rbind(dtItogo,dtF)
  
  ##make data.table arima
  ##dtF <- h[((trainLength+1):length(y)),.(firstDataWeeks,sales)]
  ##names(dtF)[2] <- c("test")
  ##dtF$name <- "arima"
  ##dtF$MAE <- fcArimaMAE
  ##dtF$forecast <- resultArima
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  
  ##dtItogo <- rbind(dtItogo,dtF)
  
  ##make data.table croston
  ##dtF <- data.table(test)
  ##dtF$name <- "croston"
  ##dtF$MAE <- fcCrostonMAE
  ##dtF$forecast <- resultCroston
  ##dtF$err <- dtF$forecast - dtF$test
  ##dtF$err2 <- dtF$err^2
  ##dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  ##dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  ##dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  ##dtItogo <- rbind(dtItogo,dtF)
  
  
  dtItogo$skuId <- skuId
  dtItogo$N <- length(y)
  dtItogo$minMAE <- min(dtItogo$MAE)
  ##dtItogo$forecast <- exp(dtItogo$forecast) - 1
  ##dtItogo$test <- exp(dtItogo$test) - 1
  
  dtItogo <- dtItogo[minMAE == MAE]
  return(dtItogo)
}

forecastOneSkuIdRandomForest <- function(h,skuId,lenghtForecast = 12) {
  
  setkey(h,materialId)
  h <- h[skuId]
  ## minus negative forecast
  h[sales <=0]$sales <- 0
  meanTrain <- mean(h$sales)
  ##h[sales == 0 & stock <=1]$sales <- meanTrain
  ##h$sales <- log(h$sales + 1)
  ## prepare Data for tain and test
  h <- h[order(firstDataWeeks)]
  ##create train data 
  tempSales <- h[order(firstDataWeeks),shift(sales,1:26,0,"lag",T)]
  tempStock <- h[order(firstDataWeeks),shift(stock,1:26,0,"lag",T)]
  h <- cbind(h,tempSales,tempStock)
  
  train <- h[21:dim(h)[1]-1][,materialId:=NULL][,firstDataWeeks:=NULL]
  test  <- h[dim(h)[1]:dim(h)[1]][,materialId:=NULL][,firstDataWeeks:=NULL]
  
  model1 <- randomForest(sales ~ ., data = train,ntree = 500, mtry = 6, importance = TRUE)
  predTrain <- predict(model1, test, type = "class")
  
  
  dtF <- h[dim(h)[1],.(firstDataWeeks,sales)]
  names(dtF)[2] <- c("test")
  dtF$name <- "rf"
  dtF$MAE <- 0
  dtF$forecast <- predTrain
  dtF$err <- dtF$forecast - dtF$test
  dtF$err2 <- dtF$err^2
  dtF$maxFS <- ifelse(dtF$forecast > dtF$test,dtF$forecast,dtF$test)
  dtF$per <- 1 - abs(dtF$err)/dtF$maxFS
  dtF$per <- ifelse(dtF$maxFS == 0 & dtF$forecast == 0,1,dtF$per)
  
  dtItogo <- dtF
  dtItogo$skuId <- skuId
  dtItogo$N <- dim(h)[1]
  dtItogo$minMAE <- min(dtItogo$MAE)
  ##dtItogo$forecast <- exp(dtItogo$forecast) - 1
  ##dtItogo$test <- exp(dtItogo$test) - 1
  
  return(dtItogo)
}


