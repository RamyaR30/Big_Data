library(rgl)

data <- read.csv("C:/Users/BigData/bdproject/data/simulationKV.csv")

rgl.open()

x <- data$failRate * 100000
y <- data$avgWriteRead
y2 <- data$missingPercent*20
z <- data$replication * 50

# Add bounding box decoration
rgl.lines(c(min(x), max(x)), c(0, 0), c(0, 0), color = "black")
rgl.lines(c(0, 0), c(min(y),max(y)), c(0, 0), color = "red")
rgl.lines(c(0, 0), c(0, 0), c(min(z),max(z)), color = "green")

rgl.points(x, y, z, r = 0.2, color = "red")
rgl.points(x, y2, z, r = 0.2, color = "gray")

rgl.texts(max(x), 0, 0, "fail rate", color =  "black", cex = 1)
rgl.texts(0, max(y), 0, "average write read", color =  "red", cex = 1)
rgl.texts(0, max(y)-100, 0, "missing", color =  "gray", cex = 1)
rgl.texts(0, 0, max(z), "replication" , color =  "green", cex = 1)
