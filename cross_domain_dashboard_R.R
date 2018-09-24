#import libraries
library(RJDBC)
library(RODBC)
library(plyr)
library(splitstackshape)
drv <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath="C:/Users/rg050776/Documents/R/win-library/3.3/vertica-jdbc-8.0.0-0.jar")

conn <- dbConnect(drv, "jdbc:vertica://CERNOCRSVERTDB-QUERY.CERNERASP.COM:5433/Cerner", "rg050776", "Devagnyay2018")

#timerdata functions to fetch data for different clients
timerdata_function <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='SGEN_OH' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M205' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function1 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='SGEN_OH' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P205' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function2 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='CERN_KCM' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='1501DEP' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function3 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='CERN_KCM' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='1501DEPM' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function4 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='LOWE_MA' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P44' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function5 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='LOWE_MA' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M44' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}

timerdata_function6 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='DUBO_PA' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P144' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function7 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='DUBO_PA' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M144' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}		
timerdata_function8 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='IHC_UT' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P351' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}		
timerdata_function9 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='IHC_UT' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M341' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}	
timerdata_function10 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='CHAR_NC' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M197' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function11 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='NKCMH_MO' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P002' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function12 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='NKCMH_MO' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M002' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function13 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='UNIV_MO' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P810' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function14 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='UNIV_MO' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M810' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function15 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='UNVR_NY' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P159' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function16 <- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='UNVR_NY' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='M159' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}

timerdata_function17<- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='CHAR_NC' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='P197' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}
timerdata_function18<- function(a,b){ 
  timerdata <- dbGetQuery(conn, "select * from (select r.timername||'&'||r.subtimername,COUNT(*) as tcount from olyprd.rtms_timer r 
                          where r.client='CHAR_NC' and r.enddatetime >= TO_DATE(?, 'YYYY-MM-DD')- INTERVAL '10 HOURS 30 MINUTES'
                          and r.enddatetime <= TO_DATE(?, 'YYYY-MM-DD') + INTERVAL '10 HOURS 30 MINUTES' and r.domain='C197' group by r.timername,r.subtimername) t order by tcount",a,b)
  return(timerdata)}

#initialize empty data frames
unvrnyp=data.frame()
unvrnym=data.frame()
univmop=data.frame()
univmom=data.frame()
nkchmmop=data.frame()
nkchmmom=data.frame()
sgenohm205=data.frame()
sgenohp205=data.frame()
cernkcm_1501dep=data.frame()
cernkcm_1501depm=data.frame()
lowe_ma_p44=data.frame()
lowe_ma_m44=data.frame()
dubo_pap44=data.frame()
dubo_pam44=data.frame()
ihcutp=data.frame()
ihcutm=data.frame()
charnc=data.frame()
charncp=data.frame()
charncc=data.frame()

#fetch daily data ..uncomment below lines for weekend data
#for (i in 3:1)
#{
a=Sys.Date()-i
sgen_oh_m205=timerdata_function(a,a)
if(nrow(sgen_oh_m205)>0)
{
  sgen_oh_m205$date=a
  sgenohm205=rbind(sgenohm205,sgen_oh_m205)
}

  
sgen_oh_p205=timerdata_function1(a,a)
if(nrow(sgen_oh_p205)>0)
{
  sgen_oh_p205$date=a
  sgenohp205=rbind(sgenohp205,sgen_oh_p205)
}
cern_1501dep=timerdata_function2(a,a)
if(nrow(cern_1501dep)>0)
{
  cern_1501dep$date=a
  cernkcm_1501dep=rbind(cernkcm_1501dep,cern_1501dep)
}
#a=Sys.Date()-i
cern_1501depm=timerdata_function3(a,a)
if(nrow(cern_1501depm)>=1)
{
  cern_1501depm$date=a
  cernkcm_1501depm=rbind(cernkcm_1501depm,cern_1501depm)
}
lowe_map44=timerdata_function4(a,a)
if(nrow(lowe_map44)>0)
{
  lowe_map44$date=a
  lowe_ma_p44=rbind(lowe_ma_p44,lowe_map44)
} 
lowe_mam44=timerdata_function5(a,a)
if(nrow(lowe_mam44)>0)
{
  lowe_mam44$date=a
  lowe_ma_m44=rbind(lowe_ma_m44,lowe_mam44)
} 
dubopap44=timerdata_function6(a,a)
if(nrow(dubopap44)>0)
{
  dubopap44$date=a
  dubo_pap44=rbind(dubo_pap44,dubopap44)
} 
dubopam44=timerdata_function7(a,a)
if(nrow(dubopam44)>0)
{
  dubopam44$date=a
  dubo_pam44=rbind(dubo_pam44,dubopam44)
} 
ihc_utp=timerdata_function8(a,a)
if(nrow(ihc_utp)>0)
{
  ihc_utp$date=a
  ihcutp=rbind(ihcutp,ihc_utp)
} 
ihc_utm=timerdata_function9(a,a)
if(nrow(ihc_utm)>0)
{
  ihc_utm$date=a
  ihcutm=rbind(ihcutm,ihc_utm)
}
char_nc=timerdata_function10(a,a)
if(nrow(char_nc)>0)
{
  char_nc$date=a
  charnc=rbind(charnc,char_nc)
}
char_ncp=timerdata_function17(a,a)
if(nrow(char_ncp)>0)
{
  char_ncp$date=a
  charncp=rbind(charncp,char_ncp)
}
char_ncc=timerdata_function18(a,a)
if(nrow(char_ncc)>0)
{
  char_ncc$date=a
  charncc=rbind(charncc,char_ncc)
}

nkchm_mo_p=timerdata_function11(a,a)
if(nrow(nkchm_mo_p)>0)
{
  nkchm_mo_p$date=a
  nkchmmop=rbind(nkchm_mo_p,nkchmmop)
}
nkchm_mo_m=timerdata_function12(a,a)
if(nrow(nkchm_mo_m)>0)
{
  nkchm_mo_m$date=a
  nkchmmom=rbind(nkchmmom,nkchm_mo_m)
}
univ_mo_m=timerdata_function14(a,a)
if(nrow(univ_mo_m)>0)
{
  univ_mo_m$date=a
  univmom=rbind(univmom,univ_mo_m)
}
univ_mo_p=timerdata_function13(a,a)
if(nrow(univ_mo_p)>0)
{
  univ_mo_p$date=a
  univmop=rbind(univmop,univ_mo_p)
}

unvr_ny_p=timerdata_function15(a,a)
if(nrow(unvr_ny_p)>0)
{
  unvr_ny_p$date=a
  unvrnyp=rbind(unvrnyp,unvr_ny_p)
}
unvr_ny_m=timerdata_function16(a,a)
if(nrow(unvr_ny_m)>0)
{
  unvr_ny_m$date=a
  unvrnym=rbind(unvrnym,unvr_ny_m)
}
#}
#check if each dataframe has count>0 and add client and domain name
if(nrow(sgenohm205)>0)
{
names(sgenohm205)=c("timer_name","count","date")
sgenohm205$client='SGEN_OH'
sgenohm205$domain='M205'
}
if(nrow(sgenohp205)>0)
{
names(sgenohp205)=c("timer_name","count","date")
sgenohp205$client='SGEN_OH'
sgenohp205$domain='P205'
}
if(nrow(cernkcm_1501depm)>0)
{
names(cernkcm_1501depm)=c("timer_name","count","date")
cernkcm_1501depm$client='CERN_KCM'
cernkcm_1501depm$domain='1501DEPM'
}
if(nrow(cernkcm_1501dep)>0)
{
names(cernkcm_1501dep)=c("timer_name","count","date")
cernkcm_1501dep$client='CERN_KCM'
cernkcm_1501dep$domain='1501DEP'
}
if(nrow(lowe_ma_p44)>0)
{
names(lowe_ma_p44)=c("timer_name","count","date")
lowe_ma_p44$client='LOWE_MA'
lowe_ma_p44$domain='P44'
}
if (nrow(lowe_ma_m44)>0)
{
names(lowe_ma_m44)=c("timer_name","count","date")
lowe_ma_m44$client='LOWE_MA'
lowe_ma_m44$domain='M44'
}
if (nrow(dubo_pap44)>0)
{
names(dubo_pap44)=c("timer_name","count","date")
dubo_pap44$client='DUBO_PA'
dubo_pap44$domain='P144'
}
if (nrow(dubo_pam44)>0)
{
names(dubo_pam44)=c("timer_name","count","date")
dubo_pam44$client='DUBO_PA'
dubo_pam44$domain='M144'
}
if (nrow(ihcutp)>0)
{
names(ihcutp)=c("timer_name","count","date")
ihcutp$client='IHC_UT'
ihcutp$domain='P351'
}
if(nrow(ihcutm)>0)
{
  names(ihcutm)=c("timer_name","count","date")
  ihcutm$client='IHC_UT'
  ihcutm$domain='M341'
}
if(nrow(charnc)>0)
{
names(charnc)=c("timer_name","count","date")
charnc$client='CHAR_NC'
charnc$domain='M197'
}
if(nrow(charncp)>0)
{
  names(charncp)=c("timer_name","count","date")
  charncp$client='CHAR_NC'
  charncp$domain='P197'
}
if(nrow(charncc)>0)
{
  names(charncc)=c("timer_name","count","date")
  charncc$client='CHAR_NC'
  charncc$domain='C197'
}
if(nrow(nkchmmom)>0)
{
names(nkchmmom)=c("timer_name","count","date")
nkchmmom$client='NKCHM_MO'
nkchmmom$domain='M002'
}
if(nrow(nkchmmop)>0)
{
names(nkchmmop)=c("timer_name","count","date")
nkchmmop$client='NKCHM_MO'
nkchmmop$domain='P002'
}
if(nrow(univmop)>0)
{
names(univmop)=c("timer_name","count","date")
univmop$client='UNIV_MO'
univmop$domain='P810'
}
if(nrow(univmom)>0)
{
names(univmom)=c("timer_name","count","date")
univmom$client='UNIV_MO'
univmom$domain='M810'
}
if(nrow(unvrnyp)>0)
{
names(unvrnyp)=c("timer_name","count","date")
unvrnyp$client='UNVR_NY'
unvrnyp$domain='P159'
}
if(nrow(unvrnym)>0)
{
names(unvrnym)=c("timer_name","count","date")
unvrnym$client='UNVR_NY'
unvrnym$domain='M159'
}
#combine the data
data=rbind(ihcutm,ihcutp,dubo_pam44,dubo_pap44,lowe_ma_p44,lowe_ma_m44,cernkcm_1501dep,cernkcm_1501depm,sgenohm205,sgenohp205,charnc,charncp,univmop,univmom,nkchmmop,nkchmmom,unvrnym,unvrnyp,charncc)
data$id=1:nrow(data)

data1=cSplit(data, splitCols = "timer_name", sep = "&", direction = "wide", drop = FALSE)
data1$timer_name_4=NULL
data1$timer_name_3=NULL
data1=data1[,c(1,2,7,8,3,4,5,6)]
names(data1)[3]=c("timer")
names(data1)[4]=c("subtimer")
#code to generate the solution name by matching timers with predefined list of timers
data1$timer=toupper(data1$timer)
data1$subtimer=toupper(data1$subtimer)
t_m2=as.data.frame(data1)
t_m2$subtimer=as.character(t_m2$subtimer)
t_m2$timer=as.character(t_m2$timer)
t_m2$subtimer[is.na(t_m2$subtimer)]<-""
mnem_final=read.csv("Mnem_final.csv")
mnem_final$timername=toupper(mnem_final$timername)
mnem_final$Subtimer=toupper(mnem_final$Subtimer)
mnem_final$timername=as.character(mnem_final$timername)
mnem_final$Subtimer=as.character(mnem_final$Subtimer)
mnem_final$solution=as.character(mnem_final$solution)            
t_m2[,9]=""
t_m2[,10]=""
t_m2[,11]=""
for(i in 1:nrow(t_m2))
{
  print(i)
  for (j in 1:nrow(mnem_final))
  {
    a=tolower(t_m2[i,3])
    b=tolower(mnem_final[j,2])
    c=tolower(t_m2[i,4])
    d=tolower(mnem_final[j,3])
    #check is subtimer is null
    if(t_m2[i,4]=="")
    {
      if(grepl(b,a))
      {
        if(nchar(b)>nchar(t_m2[i,10])  )
        {
          t_m2[i,10]=b
          t_m2[i,9]=mnem_final[j,1]
          print(mnem_final[j,1])
        }
      }
    }
    else
    {
      if(grepl(b,a) && grepl(d,c))
      {
        if(nchar(b)>nchar(t_m2[i,11])|| nchar(d)>nchar(t_m2[i,10]))
        {
          t_m2[i,10]=b
          t_m2[i,9]=mnem_final[j,1]
          t_m2[i,11]=d
          print(mnem_final[j,1])
          
          #print(bt)
        }
      }
    }
  }}



#include server and db details and insert data
psrdbhandle <- odbcDriverConnect("DRIVER={SQL Server};SERVER=CTIPROD02;DATABASE=VERTICA_TIMERS_DB;UID=ctiprod02adm;PWD=adm4SQL%p") 

for (i in 1:nrow(t_m2))
{
  sql2=paste("insert into IND_CTP_Client_Timers_Data(date,timer_name,count,client,domain,update_dt_tm,solution) values('",t_m2[i,5],"','",t_m2[i,1],"',",t_m2[i,2],",'",t_m2[i,6],"','",t_m2[i,7],"','",Sys.Date(),"','",t_m2[i,9],"')",sep="")
  sqlQuery(psrdbhandle,sql2)
}

k=sqlQuery(psrdbhandle,"update a
           set a.solution=b.solution
           from IND_CTP_Client_Timers_Data a,Mpage_Timers b
           where PARSENAME(REPLACE(REPLACE(a.timer_name,'.',''),'&','.'),2)=b.timer
           ")
k=sqlQuery(psrdbhandle,"update a
           set a.solution=b.solution
           from IND_CTP_Client_Timers_Data a,Mpage_Timers b
           where SUBSTRING(a.timer_name,(CHARINDEX('&',a.timer_name)+1),LEN(a.timer_name))='' and SUBSTRING(a.timer_name,0,CHARINDEX('&',a.timer_name,0))=b.timer
           ")
k=sqlQuery(psrdbhandle,"update a
           set a.customized_flag=b.flag
           from IND_CTP_Client_Timers_Data a,customized_timer_list b
           where a.timer_name=b.timer_name
           ")
k=sqlQuery(psrdbhandle,"update a
           set a.customized_flag=1
           from IND_CTP_Client_Timers_Data a
           where a.customized_flag is NULL
           ")

