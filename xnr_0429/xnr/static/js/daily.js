var dailyLOG_Url='/system_manage/show_log_list/?user_name='+admin;
public_ajax.call_request('get',dailyLOG_Url,dailyLOG);
function dailyLOG(data) {
    $('#loglist').bootstrapTable('load', data);
    $('#loglist').bootstrapTable({
        data:data,
        search: true,//是否搜索
        pagination: true,//是否分页
        pageSize: 5,//单页记录数
        pageList: [2,5,10,20],//分页步进值
        sidePagination: "client",//服务端分页
        searchAlign: "left",
        searchOnEnterKey: false,//回车搜索
        showRefresh: false,//刷新按钮
        showColumns: false,//列选择按钮
        buttonsAlign: "right",//按钮对齐方式
        locale: "zh-CN",//中文支持
        detailView: false,
        showToggle:false,
        sortName:'bci',
        sortOrder:"desc",
        columns: [
            // {
            //     title: "用户ID",//标题
            //     field: "user_id",//键名
            //     sortable: true,//是否可排序
            //     order: "desc",//默认排序方式
            //     align: "center",//水平
            //     valign: "middle",//垂直
            // },
            {
                title: "用户名",//标题
                field: "user_name",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    if (row.user_name==''||row.user_name=='null'||row.user_name=='unknown'||!row.user_name){
                        return row.user_id;
                    }else {
                        return row.user_name;
                    };
                }
            },
            {
                title: "登陆时间",//标题
                field: "login_time",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    if (row.login_time==''||row.login_time=='null'||
                        row.login_time=='unknown'||row.login_time.length==0||!row.login_time){
                        return '未知';
                    }else {
                        var y=[];
                        for(var j of row.login_time){
                            y.push(getLocalTime(j))
                        }
                        return y.join('<br/>');
                    };
                }
            },
            {
                title: "登录IP",//标题
                field: "login_ip",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    if (row.login_ip==''||row.login_ip=='null'||row.login_ip=='unknown'||!row.login_ip){
                        return '未知';
                    }else {
                        return row.login_ip.join('<br/>');
                    };
                }
            },
            {
                title: "操作日期",//标题
                field: "operate_date",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    if (row.operate_date==''||row.operate_date=='null'||row.operate_date=='unknown'||!row.operate_date){
                        return '未知';
                    }else {
                        return row.operate_date;
                    };
                }
            },
            {
                title: "操作内容",//标题
                field: "operate_content",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    if (row.operate_content==''||row.operate_content=='null'||row.operate_content=='unknown'||!row.operate_content){
                        return '无内容';
                    }else {
                        var con=JSON.parse(row.operate_content);
                        var str='';
                        for (var s in con){
                            str+= s +'：' +con[s]+'次<br/>';
                        }
                        return str;
                    };
                }
            },
            {
                title: "删除",//标题
                field: "",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    return '<span style="cursor:pointer;color:white;" onclick="deleteLog(\''+row.log_id+'\')"><i class="icon icon-trash"></i></span>';
                }
            },
        ],
    });
}
// 添加
function AddLogSure() {
    var user_id=$('#addLogModal .user_id').val(),user_name=$('#addLogModal .user_name').val(),
        login_time=$('#addLogModal .login_time').val(),login_ip=$('#addLogModal .login_ip').val(),
        operate_time=$('#addLogModal .operate_time').val(),operate_content=$('#addLogModal .operate_content').val();
    if (user_id||user_name||login_time||login_ip||operate_time||operate_content){
        var addLog_url='/system_manage/create_log_list/?user_id='+user_id+'&user_name='+user_name+
            '&login_time='+(Date.parse(new Date(login_time))/1000)+'&login_ip='+login_ip+
            '&operate_time='+(Date.parse(new Date(operate_time))/1000)+'&operate_content='+operate_content;
        public_ajax.call_request('get',addLog_url,successFail);
    }else {
        $('#pormpt p').text('请检查您输入的内容，不能为空。');
        $('#pormpt').modal('show');
    }
}
//删除
var login_id='';
function deleteLog(_id) {
    $('#delAgain').modal('show');
    login_id=_id;
}
function sureDelLog() {
    var delLOG_Url='/system_manage/delete_log_list/?log_id='+login_id;
    public_ajax.call_request('get',delLOG_Url,successFail);
}
function successFail(data) {
    console.log(data)
    var f='';
    if (data[0]||data||data[0][0]){
        f='操作成功';
        setTimeout(function () {
            public_ajax.call_request('get',dailyLOG_Url,dailyLOG);
        },700);
    }else {
        f='操作失败';
    }
    $('#pormpt p').text(f);
    $('#pormpt').modal('show');
}
//--------------使用统计表
//导出excel
$(".outputExcel").click(function(){
	var excel_making_url='/system_manage/get_excel_count/?start_time='+$("#start_1").val()+'&end_time='+$("#end_1").val();
    public_ajax.call_request('get',excel_making_url,excel_making_request);
});
function excel_making_request(data){
	if(data['status']){
		setTimeout(function(){
			var excel_download_url='/system_manage/download_excel/?start_time='+$("#start_1").val()+'&end_time='+$("#end_1").val();
	        //public_ajax.call_request('get',excel_download_url,excel_download);
			download_excel(excel_download_url)
		},100)
	}else {
		$('#pormpt p').text('EXCEL生成失败，请稍后重试。');
        $('#pormpt').modal('show');
	}
}
function download_excel(url) {
	    var xmlResquest = new XMLHttpRequest();
	    xmlResquest.open("GET", url, true);
	    xmlResquest.setRequestHeader("Content-type", "application/json");
	    xmlResquest.setRequestHeader("Authorization", "Bearer 6cda86e3-ba1c-4737-972c-f815304932ee");
	    xmlResquest.responseType = "blob";
	    xmlResquest.onload = function (oEvent) {

	    var content = xmlResquest.response;
	    var elink = document.createElement('a');
	    elink.download = "record.xlsx";
	    elink.style.display = 'none';
	    var blob = new Blob([content]);
	    elink.href = URL.createObjectURL(blob);
	    document.body.appendChild(elink);
	     elink.click();
	    document.body.removeChild(elink);
	    };
	     xmlResquest.send();
}
//===检测是否是admin
var isnot_url='/system_manage/is_admin/';
public_ajax.call_request('get',isnot_url,isAdmin);
function isAdmin(data){
	if(data['status']){
		$('.admin_purview').show();
		$(".ipUseTotal").show();
		$("#start_1").val($_time);
		$("#end_1").val($_time);	
		var ip_total_url='/system_manage/show_user_count/?start_time='+$_time+'&end_time='+$_time;
    	public_ajax.call_request('get',ip_total_url,IPtotalTable);
	}else{
		$('.admin_purview').hide();
		$(".ipUseTotal").hide();
	}
}
//isAdmin(1)
$("#sureTime").click(function(){
	var start=$("#start_1").val();
	var end=$("#end_1").val();
	 if (start==''||end==''){
        $('#pormpt p').text('时间不能为空。');
        $('#pormpt').modal('show');
    }else {
		var ip_total_url='/system_manage/show_user_count/?start_time='+start+'&end_time='+end;
        public_ajax.call_request('get',ip_total_url,IPtotalTable);
	}
})
function IPtotalTable(_data){
	var data=[];
	for(var k in _data){
		data.push({"name":k,"value":_data[k]})
	}
	$('#ipTotalTable').bootstrapTable('load', data);
    $('#ipTotalTable').bootstrapTable({
        data:data,
        search: true,//是否搜索
        pagination: true,//是否分页
        pageSize: 10,//单页记录数
        pageList: [2,5,10,20],//分页步进值
        sidePagination: "client",//服务端分页
        searchAlign: "left",
        searchOnEnterKey: false,//回车搜索
        showRefresh: false,//刷新按钮
        showColumns: false,//列选择按钮
        buttonsAlign: "right",//按钮对齐方式
        locale: "zh-CN",//中文支持
        detailView: false,
        showToggle:false,
        sortName:'bci',
        sortOrder:"desc",
        columns: [
            {
                title: "用户名",//标题
                field: "name",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    if (row.name==''||row.name=='null'||row.name=='unknown'||!row.name){
                        return '-';
                   	}else{
                        return row.name;
                    };
                }
            },
            {
                title: "登陆次数",//标题
                field: "value",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    if (row.value==''||row.value=='null'||row.value=='unknown'||!row.value){
                        return 0;
                    }else {
                        return row.value;
                    };
                }
            },
        ],
    });
}


