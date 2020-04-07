#######################################
## import data from blob storage
#######################################
library(AzureRMR)
library(AzureStor)
library(tidyverse)
library(data.table)
library(lubridate)
library(readr)

options(datatable.optimize=2L)
cont <- blob_container("https://korusormatek.blob.core.windows.net/data/",
                       "juOllrui6HBA1h/ozVlEE6WrQ7sWKvQVqxfZuxciMSh4OMrtZ6qt/PlCx5uGF3ToDtRjxcjN1dFrmAaoqUPRwg==")

fileName <- "/data/home/mladmin/Desktop/ormateck/data/"
tempFile <- "ИсторияЗаказов"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/order/ИсторияЗаказов*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ИсторияСпецификаций"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/specification/ИсторияСпецификаций*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ОстаткиСырья"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/rowStock/ОстаткиСырья*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ОстаткиГП"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/stock/ОстаткиГП*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ГПСИерархией"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/prod/ГПСИерархией*.csv",dest = paste0(fileName,tempFile))

tempFile <- "СырьеСИерархией"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/rowProd/СырьеСИерархией*.csv",dest = paste0(fileName,tempFile))

##################################
## read data from storage account
##################################
## История заказов (заказы без достаки, чистая потребность клиента)
orderSales <- list.files(path = "./data/ИсторияЗаказов/"
                                  ,pattern = "*.csv"
                                  ,full.names = T) %>%
  map_df(~fread(.,sep=";",header = T,colClasses = 'character'))

names(orderSales) <- c("idCheck","internal","cardId","skuId"
                       ,"shopId","storeId","date","orderRub","order","vip")
orderSales$date <- dmy_hms(orderSales$date)
orderSales$orderRub <- as.numeric(sub(",", ".", orderSales$orderRub, fixed = TRUE))
orderSales$order <- as.numeric(sub(",", ".", orderSales$order, fixed = TRUE))
orderSales[is.na(orderSales)] <- 0

orderSales <- orderSales[vip != "Да"]
orderSales[,vip:=NULL]
## спецификация
specification <- list.files(path = "./data/ИсторияСпецификаций/"
                            ,pattern = "*.csv"
                            ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(specification) <- c("date","skuId","specificationId","materialId","qnt")
specification$date <- dmy_hms(specification$date)
specification$qnt <- as.numeric(sub(",", ".", specification$qnt, fixed = TRUE))
specification <- data.table(specification)

## остатки сырья
rowStock <- list.files(path = "./data/ОстаткиСырья/"
                       ,pattern = "*.csv"
                       ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(rowStock) <- c("materialId","storeId","date","rowStock","rowReserve")
rowStock <- data.table(rowStock)
rowStock$rowStock[is.na(rowStock$rowStock)] <-0
rowStock$rowReserve[is.na(rowStock$rowReserve)] <-0
rowStock$date <- dmy_hms(rowStock$date)
rowStock$rowStock <- as.numeric(sub(",", ".", rowStock$rowStock, fixed = TRUE))
rowStock$rowReserve <- as.numeric(sub(",", ".", rowStock$rowReserve, fixed = TRUE))

## остатки ГП
stockGP <- list.files(path = "./data/ОстаткиГП/"
                      ,pattern = "*.csv"
                      ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(stockGP) <- c("skuId","storeId","date","stockGP","reserveGP")
stockGP <- data.table(stockGP)
stockGP$stockGP[is.na(stockGP$stockGP)] <-0
stockGP$reserveGP[is.na(stockGP$reserveGP)] <-0
stockGP$date <- dmy_hms(stockGP$date)
stockGP$stockGP <- as.numeric(sub(",", ".", stockGP$stockGP, fixed = TRUE))
stockGP$reserveGP <- as.numeric(sub(",", ".", stockGP$reserveGP, fixed = TRUE))

## товарная иерархия
prod <- list.files(path = "./data/ГПСИерархией/"
                   ,pattern = "*.csv"
                   ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(prod) <- c("level1","level1Id","level2","level2Id","level3","level3Id"
                 ,"level4","level4Id","level5","level5Id","level6","level6Id"
                 ,"level7","level7Id","level8","level8Id","level9","level9Id"
                 ,"level10","level10Id","level11","level11Id","level12","level12Id"
                 ,"level13","level13Id"
                 ,"skuId","skuName","group","groupId","model","modelId")
prod <- data.table(prod)


## сырье иерархия
rowProd <- list.files(path = "./data/СырьеСИерархией/"
                   ,pattern = "*.csv"
                   ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(rowProd) <- c("level1","level1Id","level2","level2Id","level3","level3Id"
                 ,"level4","level4Id","level5","level5Id","level6","level6Id"
                 ,"level7","level7Id","level8","level8Id","level9","level9Id"
                 ,"level10","level10Id","level11","level11Id","level12","level12Id"
                 ,"level13","level13Id"
                 ,"materialId","skuName")
rowProd <- data.table(rowProd)


##check data
orderSales[date >= "2019-04-01",.N,date]
stockGP[date >= "2019-04-01",.N,date]
rowStock[date >= "2019-04-01",.N,date]
specification[date >= "2019-04-01",.N,date]


## Акции создает список акций с по. как в слате, с сылкой на участвующие артикула в них.
promoShop <- list.files(path = "./Акции/"
                            ,pattern = "*.csv"
                            ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

promoShop <- data.table(promoShop)
names(promoShop) <- c("promoId","promoName","startDate","finishDate","typePromo","skuName"
                      ,"promoSkuId","promo","promoStore","minQnt","maxQnt")
promoShop$startDate <- dmy_hms(promoShop$startDate)
promoShop$finishDate <- dmy_hms(promoShop$finishDate)
promoShop$promo[is.na(promoShop$promo)]<- 0
promoShop$promo <- as.numeric(sub(",", ".", promoShop$promo, fixed = TRUE))
promoShop$promoStore[is.na(promoShop$promoStore)]<- 0
promoShop$promoStore <- as.numeric(sub(",", ".", promoShop$promoStore, fixed = TRUE))
promoShop$minQnt[is.na(promoShop$minQnt)]<- 0
promoShop$minQnt <- as.numeric(sub(",", ".", promoShop$minQnt, fixed = TRUE))
promoShop$maxQnt[is.na(promoShop$maxQnt)]<- 0
promoShop$maxQnt <- as.numeric(sub(",", ".", promoShop$maxQnt, fixed = TRUE))

## номенклатура участвующая в акции
promoSkuId <- list.files(path = "./НоменклатураАкций/"
                        ,pattern = "*.csv"
                        ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

promoSkuId <- data.table(promoSkuId)
names(promoSkuId) <- c("promoSkuId","blabla","modelId","skuIdPromo")

## create promoSkuId  like promoSkuId - skuId, withoutModel
setkey(promoSkuId,modelId)
setkey(prod,modelId)

promoSkuId <- merge(promoSkuId,prod[,.(skuId,modelId)],allow.cartesian = T,all.x = T)
promoSkuId[,newSkuId:= ifelse(is.na(skuId),skuIdPromo,skuId)]
promoSkuId <- promoSkuId[,.(promoSkuId,newSkuId)]
names(promoSkuId)[2] <- c("skuId")

setkey(promoShop,promoSkuId)
setkey(promoSkuId,promoSkuId)
promo <- merge(promoShop,promoSkuId,allow.cartesian = T)

promo[startDate >= "2019-03-01" & startDate < "2019-06-01" 
      & skuId != "00000000-0000-0000-0000-000000000000",.N,startDate][order(startDate)]





