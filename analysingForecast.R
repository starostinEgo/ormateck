## analysing % forecast
h1 <- gData2[materialId == "be2f286d-6153-11e2-ad35-ac162d78f2b8"]
tempSpecification <- h[materialId == "be2f286d-6153-11e2-ad35-ac162d78f2b8"]
tempSpecification[,.N,orderSkuId][,.N]
tempSkuId <- tempSpecification[,.N,orderSkuId]
tempStockGPA <- stockGPA[skuId %in% tempSkuId$orderSkuId & stockGP >0]
tempOrderSale <- orderSales[skuId %in% tempSkuId$orderSkuId]
tempOrderSale$firstDataWeeks <- tempOrderSale$date - wday(tempOrderSale$date,week_start = 1) +1
tempOrderSale <- tempOrderSale[,.(sales = sum(order)),.(firstDataWeeks)]

ggplot(h1,aes(x = firstDataWeeks,y = sales)) + 
  geom_point() + 
  geom_line()

ggplot(h1,aes(x = firstDataWeeks,y = sales)) + 
  geom_point() + 
  geom_line() +
  geom_line(aes(x = firstDataWeeks,y = stock))

ggplot(tempOrderSale,aes(x = firstDataWeeks,y = sales)) + 
  geom_point() + 
  geom_line()


tempSpecification$date <- ymd(tempSpecification$date)
tempSpecification$firstDateMonth <- tempSpecification$date - mday(tempSpecification$date) +1
tempDateSpecification <- tempSpecification[,.N,.(orderSkuId,firstDateMonth)][,.N,firstDateMonth]

ggplot(tempDateSpecification,aes(x = firstDateMonth,y= N)) +
  geom_point() + 
  geom_line()
