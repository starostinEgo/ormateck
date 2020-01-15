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

tempFile <- "АктуальныеТТ"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "АктуальныеТТ*.csv",dest = paste0(fileName,tempFile))

tempFile <- "Акции"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "Акции*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ГПСИерархией"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ГПСИерархией*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ДисконтныеКартыЗаказов"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ДисконтныеКартыЗаказов*.csv",dest = paste0(fileName,tempFile))

tempFile <- "НоменклатураАкций"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "НоменклатураАкций*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ИсторияСпецификаций"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ИсторияСпецификаций*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ОстаткиГП"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ОстаткиГП*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ОстаткиСырья"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ОстаткиСырья*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ПодаркиАкций"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ПодаркиАкций*.csv",dest = paste0(fileName,tempFile))

tempFile <- "РегулярныеЦены"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "РегулярныеЦены*.csv",dest = paste0(fileName,tempFile))

tempFile <- "СкидкиПоАкциямЗаказов"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "СкидкиПоАкциямЗаказов*.csv",dest = paste0(fileName,tempFile))

tempFile <- "СырьеСИерархией"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "СырьеСИерархией*.csv",dest = paste0(fileName,tempFile))

tempFile <- "ТоварныйКлассификатор"
if (file.exists(paste0(fileName,tempFile))) {unlink(paste0(fileName,tempFile),recursive = T)}
storage_multidownload(cont,src = "ТоварныйКлассификатор*.csv",dest = paste0(fileName,tempFile))

##################################
## read data from storage account
##################################
## История заказов (заказы без достаки, чистая потребность клиента)
orderSales <- list.files(path = "./ИсторияЗаказов/"
                                  ,pattern = "*.csv"
                                  ,full.names = T) %>%
  map_df(~fread(.,sep=";",header = T,colClasses = 'character'))

names(orderSales) <- c("idCheck","internal","cardId","skuId"
                       ,"shopId","storeId","date","orderRub","order")
orderSales$date <- dmy_hms(orderSales$date)
orderSales$orderRub <- as.numeric(sub(",", ".", orderSales$orderRub, fixed = TRUE))
orderSales$order <- as.numeric(sub(",", ".", orderSales$order, fixed = TRUE))
orderSales[is.na(orderSales)] <- 0

## товарная иерархия
prod <- list.files(path = "./ГПСИерархией/"
                   ,pattern = "*.csv"
                   ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(prod) <- c("level1","level1Id","level2","level2Id","level3","level3Id"
                 ,"level4","level4Id","level5","level5Id","level6","level6Id"
                 ,"level7","level7Id","level8","level8Id","level9","level9Id"
                 ,"skuId","skuName","group","groupId","model","modelId")
prod <- data.table(prod)

## спецификация
specification <- list.files(path = "./ИсторияСпецификаций/"
                            ,pattern = "*.csv"
                            ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(specification) <- c("date","skuId","specificationId","materialId","qnt")
specification$date <- dmy_hms(specification$date)
specification$qnt <- as.numeric(sub(",", ".", specification$qnt, fixed = TRUE))
specification <- data.table(specification)


## сырье иерархия
rowProd <- list.files(path = "./СырьеСИерархией/"
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


## Остатки ГП
stockGP <- list.files(path = "./ОстаткиГП/"
                            ,pattern = "*.csv"
                            ,full.names = T) %>%
  map_df(~read_csv2(., col_types = cols(.default = "c")))

names(stockGP) <- c("skuId","storeId","date","stockGP","reservGP")
stockGP$date <- dmy_hms(stockGP$date)
stockGP$stockGP <- as.numeric(sub(",", ".", stockGP$stockGP, fixed = TRUE))
stockGP$reservGP <- as.numeric(sub(",", ".", stockGP$reservGP, fixed = TRUE))

stockGP[is.na(stockGP)] <- 0
stockGP <- data.table(stockGP)

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








