package mysql

import "xorm.io/xorm"

// DbBaseDao 别的DAO 可直接继承
type DbBaseDao struct {
	Engine  *xorm.Engine
	Session *xorm.Session
}

// InitSession 初始化dao 的 Session
func (this *DbBaseDao) InitSession() {
	if this.Session == nil {
		this.Session = this.Engine.Where("")
	}
}

// SetSession 注入特定的session
func (this *DbBaseDao) SetSession(session *xorm.Session) *xorm.Session {
	this.Session = session
	return this.Session
}
