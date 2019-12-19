##################
## import data from blob storage
##################
library(AzureRMR)
library(AzureStor)
library(tidyverse)
library(data.table)
library(lubridate)
library(readr)

cont <- blob_container("https://usegeneral.blob.core.windows.net/ormateck/",
                       "lCACC23ei+qZic0lKoQUrhrDSPX9uUOBXhOmizU2E8ehC8+Q9CJDbsQRUxICxLlHsJsS4PrRdTews3Z1FFheuQ==")

fileName <- "/data/home/mladmin/Desktop/ormateck/"
tempFile <- "ИсторияЗаказов"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ИсторияЗаказов*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ИсторияОтгрузок"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ИсторияОтгрузок*.csv",dest = paste0(fileName,tempFile))

##################################
## read data from storage account
##################################
orderSales <- list.files(path = "./ИсторияЗаказов/"
                                  ,pattern = "*.csv"
                                  ,full.names = T) %>%
  map_df(~fread(.,sep=";",header = T,colClasses = 'character'))

names(orderSales) <- c("idCheck","internal","cardId","skuId"
                       ,"shopId","storeId","date","orderRub","order")
orderSales$date <- dmy_hms(orderSales$date)
orderSales$orderRub <- as.double(orderSales$orderRub)
orderSales$order <- as.double(orderSales$order)
orderSales[is.na(orderSales)] <- 0


sales <- list.files(path = "./ИсторияОтгрузок/"
                         ,pattern = "*.csv"
                         ,full.names = T) %>%
  map_df(~fread(.,sep="^",header = T,colClasses = 'character',nrows = 10))

names(sales) <- c("idCheck","internal","cardId","skuId"
                       ,"shopId","storeId","date","orderRub","order")
orderSales$date <- dmy_hms(orderSales$date)
orderSales$orderRub <- as.double(orderSales$orderRub)
orderSales$order <- as.double(orderSales$order)
orderSales[is.na(orderSales)] <- 0

## убиваем внутринние продажи
orderSales <- orderSales[!(internal == "Да" & order < 0)]


temp <- orderSales[,.(countId = .N,order = sum(order),orderRub = sum(orderRub))
                   ,.(date,internal)][order(date)]
fwrite(temp,"temp.csv")












