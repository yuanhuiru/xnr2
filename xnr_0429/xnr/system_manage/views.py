#-*- coding:utf-8 -*-
import os
import time
import json
from flask import Blueprint, url_for, render_template, request,\
                  abort, flash, session, redirect, make_response, send_file
from flask_security import roles_required, login_required,current_user
from utils import create_log_list,show_log_list,delete_log_list,\
				  create_role_authority,show_authority_list,change_authority_list,delete_authority_list,\
				  create_user_account,add_user_xnraccount,show_users_account,\
				  delete_user_account,delete_user_xnraccount,change_user_account,\
				  add_xnr_map_relationship,show_xnr_map_relationship,change_xnr_platform,\
				  delete_xnr_map_relationship,update_xnr_map_relationship,control_add_xnr_map_relationship,\
				  show_all_users_account,lookup_xnr_relation,show_all_xnr,show_user_count,get_excel_count,\
                  get_current_user_info,get_your_log_list,change_user_info,get_current_time,get_access_level_info,\
                  update_access_level_info


mod = Blueprint('system_manage', __name__, url_prefix='/system_manage')

####日志管理
#添加日志内容
#http://219.224.134.213:9209/system_manage/create_log_list/?user_id=1&user_name=admin@qq.com&login_time=1507452240*1507452241&login_ip=127.0.0.1*127.0.0.1&operate_date=2017-10-08&operate_content=(创建群体,1)*(添加虚拟人,1)
@mod.route('/create_log_list/')
def ajax_create_log_list():
	user_id=request.args.get('user_id','')
	user_name=request.args.get('user_name','')
	login_time=request.args.get('login_time','')
	login_ip=request.args.get('login_ip','')
	operate_time=int(time.time())
	operate_content=request.args.get('operate_content','')
	operate_date=request.args.get('operate_date','')
	log_info=[user_id,user_name,login_time,login_ip,operate_time,operate_content,operate_date]
	results=create_log_list(log_info)
	return json.dumps(results)

#显示日志内容
#http://219.224.134.213:9209/system_manage/show_log_list
@mod.route('/show_log_list/')
#@login_required
#@roles_required('administration')
def ajax_show_log_list():
    user_name=request.args.get('user_name','')
    if current_user.has_role('administration'):
        results=show_log_list()
        print "11111111111111111111111111111111111111111111111111111"
    else:
        results=get_your_log_list(user_name)
        print "00000000000000000000000000000000000000000000000000000"
    return json.dumps(results)


# 根据日志内容，统计操作次数 2019年6月11日
#http://219.224.134.213:9209/system_manage/show_log_list
@mod.route('/show_user_count/')
def ajax_show_user_count():
	start_time=request.args.get('start_time', '')
	end_time=request.args.get('end_time', '')
	results = show_user_count(start_time, end_time)
	return json.dumps(results)


#删除日志内容
#http://219.224.134.213:9209/system_manage/delete_log_list/?log_id=0011504922400
@mod.route('/delete_log_list/')
def ajax_delete_log_list():
	log_id=request.args.get('log_id','')
	results=delete_log_list(log_id)
	return json.dumps(results)


###权限管理
#添加权限管理数据
#http://219.224.134.213:9209/system_manage/create_role_authority/?role_name=超级管理员&description=可以使用该系统所有权限，包括创建虚拟人、虚拟人日常行为控制等
#http://219.224.134.213:9209/system_manage/create_role_authority/?role_name=业务操作员&description=仅能对已经建立的管理员的日常行为进行控制
@mod.route('/create_role_authority/')
def ajax_create_role_authority():
	role_name=request.args.get('role_name','')
	description=request.args.get('description','')
	results=create_role_authority(role_name,description)
	return json.dumps(results)

#展示权限管理内容
#http://219.224.134.213:9209/system_manage/show_authority_list
@mod.route('/show_authority_list/')
def ajax_show_authority_list():
	results=show_authority_list()
	return json.dumps(results)

#修改权限
#http://219.224.134.213:9209/system_manage/change_authority_list/?role_name=业务操作员&description=对已经建立的业务管理员的日常行为进行控制
@mod.route('/change_authority_list/')
def ajax_change_authority_list():
	role_name=request.args.get('role_name','')
	description=request.args.get('description','')
	results=change_authority_list(role_name,description)
	return json.dumps(results)

#删除权限
#http://219.224.134.213:9209/system_manage/delete_authority_list/?role_name=业务操作员
@mod.route('/delete_authority_list/')
def ajax_delete_authority_list():
	role_name=request.args.get('role_name','')
	results=delete_authority_list(role_name)
	return json.dumps(results)

###账户管理
#添加账户
#http://219.224.134.213:9209/system_manage/create_user_account/?user_id=001&user_name=admin01@qq.com&my_xnrs=WXNR0001,WXNR0002
@mod.route('/create_user_account/')
def ajax_create_user_account():
	user_id=request.args.get('user_id','')
	user_name=request.args.get('user_name','')
	my_xnrs=request.args.get('my_xnrs','').split(',')
	user_account_info=[user_id,user_name,my_xnrs]
	results=create_user_account(user_account_info)
	return json.dumps(results)

#给指定账户添加虚拟人
#http://219.224.134.213:9209/system_manage/add_user_xnraccount/?account_id=001&xnr_accountid=WXNR0002,WXNR0003
@mod.route('/add_user_xnraccount/')
def ajax_add_user_xnraccount():
	account_id=request.args.get('account_id','')
	xnr_accountid=request.args.get('xnr_accountid','').split(',')
	results=add_user_xnraccount(account_id,xnr_accountid)
	return json.dumps(results)

#显示所有账户信息
#http://219.224.134.213:9209/system_manage/show_users_account
@mod.route('/show_users_account/')
def ajax_show_users_account():
	main_user=request.args.get('main_user')
	results=show_users_account(main_user)
	return json.dumps(results)

#删除账户
#http://219.224.134.213:9209/system_manage/delete_user_account/?account_id=001
@mod.route('/delete_user_account/')
def ajax_delete_user_account():
	account_id=request.args.get('account_id','')
	results=delete_user_account(account_id)
	return json.dumps(results)

#删除指定账户的某个虚拟人
#http://219.224.134.213:9209/system_manage/delete_user_xnraccount/?account_id=001&xnr_accountid=WXNR0001
@mod.route('/delete_user_xnraccount/')
def ajax_delete_user_xnraccount():
	account_id=request.args.get('account_id','')
	xnr_accountid=request.args.get('xnr_accountid','')
	results=delete_user_xnraccount(account_id,xnr_accountid)
	return json.dumps(results)

#修改账户信息
#http://219.224.134.213:9209/system_manage/change_user_account/?user_id=001&user_name=admin02@qq.com&my_xnrs=WXNR0001,WXNR0002,WXNR0003,WXNR0004
@mod.route('/change_user_account/')
def ajax_change_user_account():
	change_detail=dict()
	change_detail['user_id']=request.args.get('user_id','')
	change_detail['user_name']=request.args.get('user_name','')
	change_detail['my_xnrs']=request.args.get('my_xnrs').split(',')
	results=change_user_account(change_detail)
	return json.dumps(results)


#虚拟人通道映射
#添加映射关系
#http://219.224.134.213:9209/system_manage/add_xnr_map_relationship/?main_user=admin@qq.com&weibo_xnr_user_no=WXNR0004&weibo_xnr_name=巨星大大&qq_xnr_user_no=QXNR0001&qq_xnr_name=维权律师
@mod.route('/add_xnr_map_relationship/')
def ajax_add_xnr_map_relationship():
	xnr_map_detail=dict()
	xnr_map_detail['main_user']=request.args.get('main_user')
	xnr_map_detail['weibo_xnr_user_no']=request.args.get('weibo_xnr_user_no')
	xnr_map_detail['qq_xnr_user_no']=request.args.get('qq_xnr_user_no')
	xnr_map_detail['weixin_xnr_user_no']=request.args.get('weixin_xnr_user_no')
	xnr_map_detail['facebook_xnr_user_no']=request.args.get('facebook_xnr_user_no')
	xnr_map_detail['twitter_xnr_user_no']=request.args.get('twitter_xnr_user_no')
	xnr_map_detail['weibo_xnr_name']=request.args.get('weibo_xnr_name')
	xnr_map_detail['qq_xnr_name']=request.args.get('qq_xnr_name')
	xnr_map_detail['weixin_xnr_name']=request.args.get('weixin_xnr_name')
	xnr_map_detail['facebook_xnr_name']=request.args.get('facebook_xnr_name')
	xnr_map_detail['twitter_xnr_name']=request.args.get('twitter_xnr_name')
	xnr_map_detail['timestamp']=int(time.time())
	results=add_xnr_map_relationship(xnr_map_detail)
	return json.dumps(results)

#查询可添加映射关系的账号
#http://219.224.134.213:9209/system_manage/control_add_xnr_map_relationship/?main_user=admin@qq.com
@mod.route('/control_add_xnr_map_relationship/')
def ajax_control_add_xnr_map_relationship():
	main_user=request.args.get('main_user')
	results=control_add_xnr_map_relationship(main_user)
	return json.dumps(results)

#显示映射关系
#http://219.224.134.213:9209/system_manage/show_xnr_map_relationship/?main_user=admin@qq.com
@mod.route('/show_xnr_map_relationship/')
def ajax_show_xnr_map_relationship():
	main_user=request.args.get('main_user')
	results=show_xnr_map_relationship(main_user)
	return json.dumps(results)

#切换通道
#http://219.224.134.213:9209/system_manage/change_xnr_platform/?origin_platform=weibo&origin_xnr_user_no=WXNR0001&new_platform=qq
@mod.route('/change_xnr_platform/')
def ajax_change_xnr_platform():
    origin_platform=request.args.get('origin_platform')
    origin_xnr_user_no=request.args.get('origin_xnr_user_no')
    new_platform=request.args.get('new_platform')
    results = change_xnr_platform(origin_platform,origin_xnr_user_no,new_platform)
    return json.dumps(results)

#删除映射关系
#http://219.224.134.213:9209/system_manage/delete_xnr_map_relationship/?xnr_map_id=admin@qq.com_1510730627
@mod.route('/delete_xnr_map_relationship/')
def ajax_delete_xnr_map_relationship():
	xnr_map_id=request.args.get('xnr_map_id')
	results=delete_xnr_map_relationship(xnr_map_id)
	return json.dumps(results)

#修改映射关系
#
@mod.route('/update_xnr_map_relationship/')
def ajax_update_xnr_map_relationship():
	xnr_map_detail=dict()
	xnr_map_id=request.args.get('xnr_map_id')
	xnr_map_detail['main_user']=request.args.get('main_user')
	xnr_map_detail['weibo_xnr_user_no']=request.args.get('weibo_xnr_user_no')
	xnr_map_detail['qq_xnr_user_no']=request.args.get('qq_xnr_user_no')
	xnr_map_detail['weixin_xnr_user_no']=request.args.get('weixin_xnr_user_no')
	xnr_map_detail['facebook_xnr_user_no']=request.args.get('facebook_xnr_user_no')
	xnr_map_detail['twitter_xnr_user_no']=request.args.get('twitter_xnr_user_no')
	xnr_map_detail['weibo_xnr_name']=request.args.get('weibo_xnr_name')
	xnr_map_detail['qq_xnr_name']=request.args.get('qq_xnr_name')
	xnr_map_detail['weixin_xnr_name']=request.args.get('weixin_xnr_name')
	xnr_map_detail['facebook_xnr_name']=request.args.get('facebook_xnr_name')
	xnr_map_detail['twitter_xnr_name']=request.args.get('twitter_xnr_name')
	xnr_map_detail['timestamp']=int(time.time())
	# print 'xnr_map_id:::',xnr_map_id,type(xnr_map_id),type(str(xnr_map_id)),type(xnr_map_detail['weibo_xnr_user_no'])
	# print 'xnr_map_detail::',xnr_map_detail,type(xnr_map_detail)
	results=update_xnr_map_relationship(xnr_map_detail,xnr_map_id)
	return json.dumps(results)


#显示所有虚拟人，用于修改
@mod.route('/show_all_xnr/')
def ajax_show_all_xnr():
	main_user = request.args.get('main_user','')
	task_id = request.args.get('task_id','')
	results = show_all_xnr(main_user,task_id)
	return json.dumps(results)


#admin用户权限展示所有虚拟人
#http://219.224.134.213:9209/system_manage/show_all_users_account
@mod.route('/show_all_users_account/')
def ajax_show_all_users_account():
	results=show_all_users_account()
	xnr_list = []
	for result in results:
		xnr_list.append(result["user_name"])
	print json.dumps(xnr_list, ensure_ascii=False)
	return json.dumps(results)


#查询映射关系
#http://219.224.134.213:9209/system_manage/lookup_xnr_relation/?origin_platform=weibo&origin_xnr_user_no=WXNR0004
@mod.route('/lookup_xnr_relation/')
def ajax_lookup_xnr_relation():
	origin_platform = request.args.get('origin_platform','')
	origin_xnr_user_no = request.args.get('origin_xnr_user_no','')
	results = lookup_xnr_relation(origin_platform,origin_xnr_user_no)
	return json.dumps(results)


# 生成excel
@mod.route('/get_excel_count/')
def ajax_get_excel_count():
	start_time=request.args.get('start_time', '')
	end_time=request.args.get('end_time', '')
	results = get_excel_count(start_time, end_time)
	return json.dumps(results)


@mod.route('/download_excel/')
def index():
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    excel_name = start_time + "+" + end_time
    filename = 'static/doc/' + excel_name +'.xlsx'
    try:
        response = make_response(send_file(filename))
    except Exception as e:
        print e
        return json.dumps({"status":0})
    return response


@mod.route('/get_current_user_info/')
def ajax_get_current_user_info():
	current_user_name = request.args.get('user_name','')
	results = get_current_user_info(current_user_name)
	return json.dumps(results)

@mod.route('/change_user_info/')
def ajax_change_user_info():
	user_name = request.args.get('user_name','')
	new_password = request.args.get('new_password','')
	new_department = request.args.get('new_department','')
	confirmedat = get_current_time()
	results = change_user_info(user_name, new_password, new_department, confirmedat)
	return json.dumps(results)


@mod.route('/is_admin/')
def verify_admin():
    try:
        if current_user.has_role('administration'):
            return json.dumps({"status":1})
        else:
            return json.dumps({"status":0})
    except Exception as e:
        print e
        return json.dumps({"status":0})
 

@mod.route('/get_access_level/')
def get_access_level():
    try:
        account_name = request.args.get('account_name', '')
       
        level_info = get_access_level_info(account_name)       
        return json.dumps(level_info)
 
    except Exception as e:
        print e
        return json.dumps({"status": 'fail'})


@mod.route('/update_access_level/')
def update_access_level():
    try:
        account_name = request.args.get('account_name', '')
        access_level = request.args.get('access_level', '')
        results = update_access_level_info(account_name, access_level)

        return json.dumps(results)

    except Exception as e:
        print e
        return json.dumps({"status": 'fail'})


