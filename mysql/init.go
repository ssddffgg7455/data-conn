package mysql

import (
	"context"
	"errors"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cast"
	"github.com/ssddffgg7455/configutil"
	"github.com/ssddffgg7455/logger"
	"xorm.io/xorm"
	"xorm.io/xorm/log"
)

var engines sync.Map

// Init 初始化 db
func Init() error {
	tag := "dbdao.Init"
	instance, ok := configutil.GetConf("mysql_cluster").(map[string]interface{})
	if !ok {
		return errors.New("config mysql_cluster Format error")
	}

	for instanceName, instanceConns := range instance {
		instanceConns, ok := instanceConns.(map[string]interface{})
		if !ok {
			return errors.New("config instance Conns Format error")
		}

		var master string
		var slaves []string
		for key, ins := range instanceConns {
			if key == "master" {
				master = cast.ToString(ins)
			} else {
				slaves = append(slaves, cast.ToString(ins))
			}
		}

		conns := append([]string{master}, slaves...)
		err := _connectByConns(instanceName, conns)
		if err != nil {
			err = _connectByConns(instanceName, conns)
		}
		if err != nil {
			logger.Errorw(context.Background(), tag, "err", err)
			return err
		}

		go _monitorConnection(instanceName)
	}

	return nil
}

// GetDbInstance 获取用户实例
func GetDbInstance(instanceName, cluster string) *xorm.Engine {
	engineVal, ok := engines.Load(instanceName)
	if !ok {
		return nil
	}

	engine, ok := engineVal.(*xorm.EngineGroup)
	if !ok {
		return nil
	}

	if cluster == "master" {
		return engine.Master()
	}

	if cluster == "slave" {
		return engine.Slave()
	}

	return engine.Master()
}

// RegisterDbInstance 注入Engine 用于mock 测试
func RegisterDbInstance(instanceName string, engine *xorm.EngineGroup) {
	engines.Store(instanceName, engine)
}

func _connect(instanceName string) error {
	tag := "dbdao._connect"
	instance := configutil.GetConf("mysql_cluster", instanceName)
	instanceConns, ok := instance.(map[string]interface{})
	if !ok {
		logger.Errorw(context.Background(), tag, "Config mysql_cluster Format error, instanceConns", instanceConns)
		return nil
	}

	var master string
	var slaves []string
	for key, ins := range instanceConns {
		if key == "master" {
			master = cast.ToString(ins)
		} else {
			slaves = append(slaves, cast.ToString(ins))
		}
	}

	conns := append([]string{master}, slaves...)
	return _connectByConns(instanceName, conns)
}

func _connectByConns(instanceName string, conns []string) error {
	engine, err := xorm.NewEngineGroup("mysql", conns)
	if err != nil {
		engine.Close()
		return err
	}

	if cast.ToBool(configutil.GetConf("mysql_config", "log")) {
		logWriter, err := logger.GetWriter(_getLoggerConfig())
		if err != nil {
			engine.Close()
			return err
		}
		logger := log.NewSimpleLogger(logWriter)
		logger.ShowSQL(cast.ToBool(configutil.GetConf("mysql_config", "show_sql")))
		engine.SetLogger(logger)
		engine.SetLogLevel(_getLoggerLevel())
	} else if cast.ToBool(configutil.GetConf("mysql_config", "show_sql")) {
		engine.ShowSQL(true)
	}

	if maxIdleConns := cast.ToInt(configutil.GetConf("mysql_config", "max_idle_conns")); maxIdleConns > 0 {
		engine.SetMaxIdleConns(maxIdleConns)
	}
	if maxOpenConns := cast.ToInt(configutil.GetConf("mysql_config", "max_open_conns")); maxOpenConns > 0 {
		engine.SetMaxOpenConns(maxOpenConns)
	}
	if connMaxLifetime := cast.ToInt64(configutil.GetConf("mysql_config", "conn_max_lifetime")); connMaxLifetime > 0 {
		engine.SetConnMaxLifetime(time.Duration(connMaxLifetime) * time.Second)
	}

	engines.Store(instanceName, engine)

	return nil
}

func _monitorConnection(instanceName string) {
	tag := "dbdao._monitorConnection"

	for {
		engineVal, ok := engines.Load(instanceName)
		if !ok {
			_connect(instanceName)
			time.Sleep(time.Second)
			continue
		}
		engine, ok := engineVal.(*xorm.EngineGroup)
		if !ok {
			_connect(instanceName)
			time.Sleep(time.Second)
			continue
		}

		err := engine.Ping()
		if err != nil {
			engine.Close()
			logger.Errorw(context.Background(), tag, "err", err)
			_connect(instanceName)
		}
		time.Sleep(time.Second)
	}
}

func _getLoggerConfig() *logger.Config {
	return &logger.Config{
		Level:        cast.ToString(configutil.GetConf("mysql_log", "level")),        // 日志分级 DEBUG, INFO, WARN, ERROR, DPANIC, PANIC, FATAL
		LogFile:      cast.ToString(configutil.GetConf("mysql_log", "log_file")),     // 文件名 不带后缀（.log）
		LogPath:      cast.ToString(configutil.GetConf("mysql_log", "log_path")),     // 日志路径
		MaxAge:       cast.ToInt(configutil.GetConf("mysql_log", "max_age")),         // 最大保存时间 单位 day
		RotationSize: cast.ToInt64(configutil.GetConf("mysql_log", "rotation_size")), // 日志文件滚动size 单位 M
		RotationTime: cast.ToInt(configutil.GetConf("mysql_log", "rotation_time")),   // 日志滚动周期 单位 hour
	}
}

var levelMap = map[string]log.LogLevel{
	"DEBUG": log.LOG_DEBUG,
	"INFO":  log.LOG_INFO,
	"WARN":  log.LOG_WARNING,
	"ERROR": log.LOG_ERR,
}

func _getLoggerLevel() log.LogLevel {
	level := cast.ToString(configutil.GetConf("mysql_log", "level"))
	if logLevel, ok := levelMap[level]; ok {
		return logLevel
	}
	return log.LOG_UNKNOWN
}
