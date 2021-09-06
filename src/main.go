package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"gopkg.in/gomail.v2"
	"gopkg.in/ini.v1"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"strings"
	"time"
)

type Partition struct {
	TableSchema string
	TableName  string
	PartitionName string
	partition_description  string
}

func init() {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.DisableQuote = true
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000"
	logrus.SetFormatter(customFormatter)
	logrus.SetOutput(&lumberjack.Logger{
		Filename: "partition.log",
		MaxSize:  500, //M
		MaxAge:   30,  //days
	})

}

func sendMail(body string){
	cfgserver, err := ini.Load("config.ini")
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
	host := cfgserver.Section("mail").Key("host").Value()
	username := cfgserver.Section("mail").Key("username").Value()
	password := cfgserver.Section("mail").Key("password").Value()
	recipients := cfgserver.Section("mail").Key("recipients").Value()
	subject := cfgserver.Section("mail").Key("subject").Value()
	m := gomail.NewMessage()
	m.SetHeader("From",username)
	recvArr := strings.Split(recipients,",")
	addresses := make([]string, len(recvArr))
	for i, recipient := range recvArr {
		addresses[i] = m.FormatAddress(recipient, "")
	}
	m.SetHeader("To", addresses...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", body)
	d := gomail.NewDialer(host, 465, username, password)
	err2 := d.DialAndSend(m)
	if err2 != nil{
		logrus.Error(err2)
	}
	logrus.Info("邮件发送成功")
}

func GetDB(url string) *sql.DB {
	db, err := sql.Open("mysql", url)
	if err != nil {
		panic(err)
	}
	return db
}

func GetCurrentPartitionSql() string {
	sqlstr := "select"+
		" p.TABLE_SCHEMA,"+
		" p.TABLE_NAME,"+
		" p.PARTITION_NAME,"+
		" p.PARTITION_DESCRIPTION"+
		" from"+
		" information_schema.PARTITIONS p"+
		" where"+
		" p.PARTITION_NAME <> 'p_nulls'"+
		" and p.TABLE_SCHEMA = ?"+
		" and p.TABLE_NAME = ?"+
		" order by p.PARTITION_ORDINAL_POSITION desc"+
		" limit 1"
	return sqlstr
}

func ConvertToDtSql() string{
	sqlstr := "select date_format(from_days(?),'%Y%m%d') as days"
	return sqlstr
}

func AddPartition(db *sql.DB,table_schema string,table_name string,partition_name string,dt string){
	sqlstr := "alter table "+table_name +" add partition (partition "+partition_name+" values less than (to_days('"+dt+"')))"
	_,err := db.Exec(sqlstr)
	if err != nil {
		fmt.Println("添加分区失败", err)
		os.Exit(1)
	}
	fmt.Printf("添加分区(库:%s,表:%s,分区:%s)\n",table_schema,table_name,partition_name)
	logrus.Infof("添加分区(库:%s,表:%s,分区:%s)",table_schema,table_name,partition_name)
}

func GetOneYearDT() string{
	sqlstr := "SELECT date_format(date_add(now(), INTERVAL 1 YEAR),'%Y%m%d') as dt"
	return sqlstr
}

func main() {

	cfgserver, err := ini.Load("config.ini")
	if err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
	url := cfgserver.Section("server").Key("url").Value()
	months,_ := cfgserver.Section("table").Key("months").Int()
	table_schema := cfgserver.Section("table").Key("table_schema").Value()
	table_names := cfgserver.Section("table").Key("table_names").Value()
	tableArr := strings.Split(table_names,",")
	db := GetDB(url)
	defer db.Close()
	var futureDt string
	dtrow := db.QueryRow(GetOneYearDT())
	//获取1年后的时间(年月日)
	dtrow.Scan(&futureDt)
	futureDate,_ := time.Parse("20060102", futureDt)
	currPartSql := GetCurrentPartitionSql()
	var sb strings.Builder
	for _, table_name := range tableArr {
		var part Partition
		//获取表当前最后一个分区信息
		rows1 := db.QueryRow(currPartSql,table_schema,table_name)
		if err := rows1.Scan(&part.TableSchema,&part.TableName,&part.PartitionName,&part.partition_description); err != nil {
			if err == sql.ErrNoRows {
				msgerr := fmt.Errorf("找不到分区表,库:%s,表:%s", table_schema,table_name)
				fmt.Println(msgerr.Error())
				logrus.Error(msgerr.Error())
				os.Exit(1)
			}
		}
		rows2 := db.QueryRow(ConvertToDtSql(),part.partition_description)
		var curPartDt string
		//获取最后一个分区的日期(年月日)
		rows2.Scan(&curPartDt)
		//添加分区
		curPartDate, _ := time.Parse("20060102", curPartDt)
		for {
			currentYear, currentMonth, _ := curPartDate.Date()
			firstOfMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, time.UTC)
			nextDt := firstOfMonth.AddDate(0,months,0)
			if nextDt.After(futureDate){
				break
			}
			day := nextDt.Format("20060102")
			partName := "p"+day
			AddPartition(db,table_schema,table_name,partName,day)
			sb.WriteString("添加分区(库:"+table_schema+",表:"+table_name+",分区:"+partName+")<br/>")
			curPartDate = nextDt
		}
	}

	if len(sb.String()) !=0 {
		sendMail(sb.String())
	}
}

