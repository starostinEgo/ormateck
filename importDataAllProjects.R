#######################################
## import data from blob storage
## and create rds format
#######################################
library(AzureRMR)
library(AzureStor)
library(tidyverse)
library(data.table)
library(lubridate)
library(readr)

cont <- blob_container("https://korusormatek.blob.core.windows.net/data/",
                       "juOllrui6HBA1h/ozVlEE6WrQ7sWKvQVqxfZuxciMSh4OMrtZ6qt/PlCx5uGF3ToDtRjxcjN1dFrmAaoqUPRwg==")
cont2 <- blob_container("https://usegeneral.blob.core.windows.net/ormateck/",
                        "lCACC23ei+qZic0lKoQUrhrDSPX9uUOBXhOmizU2E8ehC8+Q9CJDbsQRUxICxLlHsJsS4PrRdTews3Z1FFheuQ==")

fileName <- "/data/home/mladmin/Desktop/ormateck/data/"
tempFile <- "ИсторияЗаказов"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/order/ИсторияЗаказов*.csv",dest = paste0(fileName,tempFile))

fileName2 <- "/data/home/mladmin/Desktop/ormateck/data2/"
tempFile <- "ИсторияЗаказов"
if (file.exists(paste0(fileName2,tempFile))) {unlink(paste0(fileName2,tempFile),recursive = T)}
storage_multidownload(cont2,src = "ИсторияЗаказов*.csv",dest = paste0(fileName2,tempFile))

tempFile <- "ИсторияСпецификаций"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/specification/ИсторияСпецификаций*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ИсторияСпецификаций"
if (file.exists(paste0(fileName2,tempFile))) {unlink(paste0(fileName2,tempFile),recursive = T)}
storage_multidownload(cont2,src = "ИсторияСпецификаций*.csv",dest = paste0(fileName2,tempFile))

tempFile <- "ОстаткиСырья"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/rowStock/ОстаткиСырья*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ОстаткиСырья"
if (file.exists(paste0(fileName2,tempFile))) {unlink(paste0(fileName2,tempFile),recursive = T)}
storage_multidownload(cont2,src = "ОстаткиСырья*.csv",dest = paste0(fileName2,tempFile))

tempFile <- "ОстаткиГП"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/stock/ОстаткиГП*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ОстаткиГП"
if (file.exists(paste0(fileName2,tempFile))) {unlink(paste0(fileName2,tempFile),recursive = T)}
storage_multidownload(cont2,src = "ОстаткиГП*.csv",dest = paste0(fileName2,tempFile))

tempFile <- "ГПСИерархией"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/prod/ГПСИерархией*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ГПСИерархией"
if (file.exists(paste0(fileName2,tempFile))) {unlink(paste0(fileName2,tempFile),recursive = T)}
storage_multidownload(cont2,src = "ГПСИерархией*.csv",dest = paste0(fileName2,tempFile))

tempFile <- "СырьеСИерархией"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "rowData/rowProd/СырьеСИерархией*.csv",dest = paste0(fileName,tempFile))

tempFile <- "СырьеСИерархией"
if (file.exists(paste0(fileName2,tempFile))) {unlink(paste0(fileName2,tempFile),recursive = T)}
storage_multidownload(cont2,src = "СырьеСИерархией*.csv",dest = paste0(fileName2,tempFile))

##################################
## read data from storage account
##################################
## История заказов (заказы без достаки, чистая потребность клиента)
orderSales <- list.files(path = "./data/ИсторияЗаказов/"
                                  ,pattern = "*.csv"
                                  ,full.names = T) %>%
  map_df(~fread(.,sep=";",header = T,colClasses = 'character'))

names(orderSales) <- c("idCheck","internal","cardId","skuId"
                       ,"shopId","storeId","date","orderRub","order")
orderSales$date <- dmy_hms(orderSales$date)
orderSales$orderRub <- as.numeric(sub(",", ".", orderSales$orderRub, fixed = TRUE))
orderSales$order <- as.numeric(sub(",", ".", orderSales$order, fixed = TRUE))
orderSales[is.na(orderSales)] <- 0


orderSales2 <- list.files(path = "./data2/ИсторияЗаказов/"
                         ,pattern = "*.csv"
                         ,full.names = T) %>%
  map_df(~fread(.,sep=";",header = T,colClasses = 'character'))

names(orderSales2) <- c("idCheck","internal","cardId","skuId"
                       ,"shopId","storeId","date","orderRub","order")
orderSales2$date <- dmy_hms(orderSales2$date)
orderSales2$orderRub <- as.numeric(sub(",", ".", orderSales2$orderRub, fixed = TRUE))
orderSales2$order <- as.numeric(sub(",", ".", orderSales2$order, fixed = TRUE))
orderSales2[is.na(orderSales2)] <- 0

## товарная иерархия
prod <- list.files(path = "./data/ГПСИерархией/"
                   ,pattern = "*.csv"
                   ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(prod) <- c("level1","level1Id","level2","level2Id","level3","level3Id"
                 ,"level4","level4Id","level5","level5Id","level6","level6Id"
                 ,"level7","level7Id","level8","level8Id","level9","level9Id"
                 ,"skuId","skuName","group","groupId","model","modelId")
prod <- data.table(prod)


prod2 <- list.files(path = "./data2/ГПСИерархией/"
                   ,pattern = "*.csv"
                   ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(prod2) <- c("level1","level1Id","level2","level2Id","level3","level3Id"
                 ,"level4","level4Id","level5","level5Id","level6","level6Id"
                 ,"level7","level7Id","level8","level8Id","level9","level9Id"
                 ,"skuId","skuName","group","groupId","model","modelId")
prod2 <- data.table(prod2)

## спецификация
specification <- list.files(path = "./data/ИсторияСпецификаций/"
                            ,pattern = "*.csv"
                            ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(specification) <- c("date","skuId","specificationId","materialId","qnt")
specification$date <- dmy_hms(specification$date)
specification$qnt <- as.numeric(sub(",", ".", specification$qnt, fixed = TRUE))
specification <- data.table(specification)

specification2 <- list.files(path = "./data2/ИсторияСпецификаций/"
                            ,pattern = "*.csv"
                            ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(specification2) <- c("date","skuId","specificationId","materialId","qnt")
specification2$date <- dmy_hms(specification2$date)
specification2$qnt <- as.numeric(sub(",", ".", specification2$qnt, fixed = TRUE))
specification2 <- data.table(specification2)

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

rowProd2 <- list.files(path = "./data2/СырьеСИерархией/"
                      ,pattern = "*.csv"
                      ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(rowProd2) <- c("level1","level1Id","level2","level2Id","level3","level3Id"
                    ,"level4","level4Id","level5","level5Id","level6","level6Id"
                    ,"level7","level7Id","level8","level8Id","level9","level9Id"
                    ,"level10","level10Id","level11","level11Id","level12","level12Id"
                    ,"level13","level13Id"
                    ,"materialId","skuName")
rowProd2 <- data.table(rowProd2)

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

stockGP2 <- list.files(path = "./data2/ОстаткиГП/"
                      ,pattern = "*.csv"
                      ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(stockGP2) <- c("skuId","storeId","date","stockGP","reserveGP")
stockGP2 <- data.table(stockGP2)
stockGP2$stockGP[is.na(stockGP2$stockGP)] <-0
stockGP2$reserveGP[is.na(stockGP2$reserveGP)] <-0
stockGP2$date <- dmy_hms(stockGP2$date)
stockGP2$stockGP <- as.numeric(sub(",", ".", stockGP2$stockGP, fixed = TRUE))
stockGP2$reserveGP <- as.numeric(sub(",", ".", stockGP2$reserveGP, fixed = TRUE))
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

rowStock2 <- list.files(path = "./data2/ОстаткиСырья/"
                       ,pattern = "*.csv"
                       ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(rowStock2) <- c("materialId","storeId","date","rowStock","rowReserve")
rowStock2 <- data.table(rowStock2)
rowStock2$rowStock[is.na(rowStock2$rowStock)] <-0
rowStock2$rowReserve[is.na(rowStock2$rowReserve)] <-0
rowStock2$date <- dmy_hms(rowStock2$date)
rowStock2$rowStock <- as.numeric(sub(",", ".", rowStock2$rowStock, fixed = TRUE))
rowStock2$rowReserve <- as.numeric(sub(",", ".", rowStock2$rowReserve, fixed = TRUE))


##check data
orderSales[date >= "2019-04-01",.N,date]
stockGP[date >= "2019-04-01",.N,date]
rowStock[date >= "2019-04-01",.N,date]
specification[date >= "2019-04-01",.N,date]

##sales <- list.files(path = "./ИсторияОтгрузок/"
##                         ,pattern = "*.csv"
##                         ,full.names = T) %>%
##  map_df(~fread(.,sep="^",header = T,colClasses = 'character',nrows = 10))

##sales <- list.files(path = "./ИсторияОтгрузок/"
##                         ,pattern = "*.csv"
##                         ,full.names = T) %>%
##  map_df(~read_csv2(., col_types = cols(.default = "c")))

##sales <- data.table(sales)
##sales[is.na(sales)] <- 0
##names(sales) <- c("idCheck","cardId","skuId"
##                       ,"shopId","date","salesRub","sales")
##sales$date <- dmy_hms(sales$date)
##sales$salesRub <- as.numeric(sub(",", ".", sales$salesRub, fixed = TRUE))
##sales$sales <- as.numeric(sub(",", ".", sales$sales, fixed = TRUE))


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
## Остатки сырья
stock <- list.files(path = "./ОстаткиСырья/"
                      ,pattern = "*.csv"
                      ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(stock) <- c("skuId","storeId","date","stock","reserv")
stock$date <- dmy_hms(stock$date)
stock$stock <- as.numeric(sub(",", ".", stock$stock, fixed = TRUE))
stock$reserv <- as.numeric(sub(",", ".", stock$reserv, fixed = TRUE))

stock[is.na(stock)] <- 0
stock <- data.table(stock)


## сырье иерархия
prod <- list.files(path = "./СырьеСИерархией/"
                   ,pattern = "*.csv"
                   ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(prod) <- c("level1","level1Id","level2","level2Id","level3","level3Id"
                 ,"level4","level4Id","level5","level5Id","level6","level6Id"
                 ,"level7","level7Id","level8","level8Id","level9","level9Id"
                 ,"skuId","skuName")
prod <- data.table(prod)


orderSales[,.N,date]
setkey(orderSales,skuId)
setkey(prod,skuId)
temp <- merge(orderSales,prod)





