var relatedUrl='/weibo_xnr_operate/related_recommendation/?xnr_user_no='+ID_Num+'&sort_item=influence';
public_ajax.call_request('get',relatedUrl,related);
var idBox='influence',idBoxZONG='influe';
function related(data) {
    $.each(data,function (index,item) {
        detList[item.uid]=item;
    });
    $('#'+idBox).bootstrapTable('load', data);
    $('#'+idBox).bootstrapTable({
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
                title: "编号",//标题
                field: "",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
					var chk=fs_uid_list.indexOf(row.uid)==-1?'':'checked';
                    return '<input type="checkbox" name="'+idBox+'" '+chk+'  onclick="joinFS(\''+row.uid+'\')" style="position:relative;top:2px;right:5px;"/>&nbsp;'+ (index+1);
                }
            },
            {
                title: "用户UID",//标题
                field: "uid",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
            },
            {
                title: "敏感度",//标题
                field: "sensitive",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                visible:false
            },
            {
                title: "粉丝数",//标题
                field: "fansnum",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直

            },
            {
                title: "好友数",//标题
                field: "friendsnum",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直

            },
            {
                title: "微博数",//标题
                field: "statusnum",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
            },
            {
                title: '操作',//标题
                field: "",//键名
                sortable: true,//是否可排序
                order: "desc",//默认排序方式
                align: "center",//水平
                valign: "middle",//垂直
                formatter: function (value, row, index) {
                    var fol='',_isEmpty='';
                    if (row.weibo_type=='follow'){
                        fol='已关注，点击取消关注';_isEmpty='';
                    }else if (row.weibo_type=='friends'){
                        fol='相互关注，点击取消对他关注';_isEmpty='';
                    }else {//if (row.weibo_type=='stranger'||row.weibo_type=='followed')
                        fol='未关注，点击直接关注';_isEmpty='-empty';
                    }
                    return '<span style="cursor: pointer;" onclick="lookDetails(\''+row.uid+'\')" title="查看详情"><i class="icon icon-file-alt"></i></span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'+
                        '<span style="cursor: pointer;" onclick="driectFocus(\''+row.uid+'\',this)" title="'+fol+'"><i class="jiaStar icon icon-star'+_isEmpty+'"></i></span>';
                },
            },
        ],
		/*onPostBody:function(){
			var trList= $('#'+idBox+' tbody tr');
			for(var a=0;a<trList.length;a++){
				let td_2=$(trList[a]).find('td').eq(1).text();
				let b=onceFocusObj[td_2];
				if(b==1){
					$(trList[a]).find('.jiaStar').removeClass('icon-star-empty').addClass('icon-star');
				}else {
					$(trList[a]).find('.jiaStar').removeClass('icon-star').addClass('icon-star-empty');
				}
            }
		},*/
    });
    $('#'+idBoxZONG+' p').slideUp(700);
    $('.'+idBox).show();
}
$('#influence').bootstrapTable('hideColumn', 'sensitive');
function showHide(_tp$) {
    if (_tp$=='sensitive'){
        $('#influence').bootstrapTable('showColumn', 'sensitive');
    }else {
        $('#influence').bootstrapTable('hideColumn', 'sensitive');
    }
};
$('#myTabs li').on('click',function () {
    fs_uid_list=[];
	var ty=$(this).attr('num'),urlREL;
    //idNAME=ty;
	if(ty==1){
		idBox='influence',idBoxZONG='influe';
		 $('.influence').hide();
  	    $('#influe p').show();
		var ytl=$('#inputList input:radio[name="xnr123"]:checked').attr('tp');
		urlREL='/weibo_xnr_operate/related_recommendation/?xnr_user_no='+ID_Num+'&sort_item='+ytl;				
		showHide(ytl);
	}else {
		idBox='influence2',idBoxZONG='influe2';
		$('.influence2').hide();
    	$('#influe2 p').show();
		urlREL='/weibo_xnr_operate/daily_recomment_tweets/?xnr_user_no='+ID_Num+'&sort_item=user_index';
	}
    public_ajax.call_request('get',urlREL,related);
 
})
//批量关注
var fs_uid_list=[];
function joinFS(uid){
	var _i=fs_uid_list.indexOf(uid);
	if(_i == (-1) ){
		fs_uid_list.push(uid);
	}else {
		fs_uid_list.splice(_i,1);
	}
}
$("#allFoucs").on('click',function(){
	if(fs_uid_list.length==0){
		$('#pormpt p').text('请选择要关注的人。');
        $('#pormpt').modal('show');
	}else {
		$("#focusListType").modal('show');
	}
});
function driectFocus(uid,_this){
	joinFS(uid);
	setTimeout(function(){
		focus_this_uid=fs_uid_list;
		$("#focusListType").modal('show');
	},300);
}
/*$("#sureFlist").click(function(){
	var _type=[];
    $("#focusListType input:checkbox:checked").each(function (index,item) {
        _type.push($(this).val());
    });
	if(fs_uid_list.length==0||_type.length==0){
        $('#pormpt p').text('请选择要关注的人和要关注的类型。');
        $('#pormpt').modal('show');
        return false;
    }
    var focus_list_url='/weibo_xnr_operate/follow_operate/?xnr_user_no='+ID_Num+
            '&uid='+fs_uid_list.join('，')+'&follow_type='+_type.join('，');
    public_ajax.call_request('get',focus_list_url,suc__Fai);

})
function suc__Fai(data){
    var _in=data['status'];
    var txt= _in=='ok'?'操作成功':'操作失败';
    $('#pormpt p').text(txt);
    $('#pormpt').modal('show');
}*/
$('#inputList label input').on('click',function () {
    var ty=$(this).attr('tp');
    //idNAME=ty;
    $('.influence').hide();
    $('#influe p').show();
    var relatedUrl='/weibo_xnr_operate/related_recommendation/?xnr_user_no='+ID_Num+'&sort_item='+ty;
    public_ajax.call_request('get',relatedUrl,related);
    showHide(ty);
})
//直接搜索
$('.findSure').on('click',function () {
    var ids=$('.active-1-find').val();
    if (ids==''){
        $('#pormpt p').text('搜索内容不能为空。');
        $('#pormpt').modal('show');
    }else {
        ids=ids.replace(/,/g,'，');
        idNAME='searchResult';
        $('.influence').hide();
        $('#influe p').show();
        var searchUrl='/weibo_xnr_operate/direct_search/?xnr_user_no='+ID_Num+'&sort_item=influence&uids='+ids;
        public_ajax.call_request('get',searchUrl,related);
        $('.searchResult').slideDown(30);
    }
});
//查看详情
var detList={};
function lookDetails(puid) {
    var person=detList[puid];
    if (person.portrait_status== true){
        var name,img,gender,domain,location,topic_string,weibo_type;
        if (person.uname==''||person.uname=='unknown'||person.uname=='null'){
            name='未知';
        }else {
            name=person.uname;
        }
        if (person.photo_url==''||person.photo_url=='unknown'||person.photo_url=='null'){
            img='/static/images/unknown.png';
        }else {
            img=person.photo_url;
        }
        if (person.domain==''||person.domain=='unknown'||person.domain=='null'){
            domain='未知';
        }else {
            domain=person.domain;
        }
        if (person.location==''||person.location=='unknown'||person.location=='null'){
            location='未知';
        }else {
            location=person.location;
        }
        if (person.topic_string==''||person.topic_string=='unknown'||person.topic_string=='null'){
            topic_string='未知';
        }else {
            topic_string=person.topic_string.replace(/&/g,'-');
        }
        if (person.gender==1){gender='男'}else if (person.gender==2){gender='女'}else{gender='未知'}
        if (person.weibo_type=='follow'){
            weibo_type='已关注';
        }else if (person.weibo_type=='friends'){
            weibo_type='相互关注';
        }else {//if (person.weibo_type=='stranger'||person.weibo_type=='followed')
            weibo_type='未关注';
        }
        $('#details .uid').text(person.uid);
        $('#details .details-name').text(name);
        $('#details .det11').text(name).attr('title',name);
        $('#details .det22').text(domain).attr('title',domain);
        $('#details .det33').text(location).attr('title',location);
        $('#details .det44').text(gender).attr('title',gender);
        $('#details .addFOCUS b').text(weibo_type);
        $('#details .det55').text(topic_string).attr('title',topic_string);
        $('#details .det-2-num').text(Math.ceil(person.influence));
        $('#details .headImg').attr('src',img);

    };
    if (person.portrait_status== false){
        $('#details .details-name').html('该人物未入库，暂无画像信息');
        $('#details .details-1').hide();
        $('#details .details-2').hide();
        $('#details .addFOCUS').hide();
    }
    var str='';
    if (person.weibo_list.length==0){
        str='暂无代表微博';
    }else {
        $.each(person.weibo_list,function (index,item) {
            str+=
                '<div><i class="icon icon-tint"></i>&nbsp;<b class="det-3-content">'+item.text+'</b></div>'
        })
    };

    $('#details .det-3-info').html(str);
    $('#details').modal('show');
}
//直接关注
/*var onceFocusObj={};
function driectFocus(uid,_this) {
    var foc_url,mid='';
    if (!uid){uid=$(_this).prev().text()}
    var f=$(_this).attr('title');
    if (f=='未关注，点击直接关注'){
        mid='follow_operate';
    }else {
        mid='unfollow_operate';
    }
    foc_url='/weibo_xnr_operate/'+mid+'/?xnr_user_no='+ID_Num+'&uid='+uid;
    public_ajax.call_request('get',foc_url,sucFai);
	setTimeout(function(){
		if(uid in onceFocusObj){
     		onceFocusObj[uid]=0;
       	 	$(_this).find('i').removeClass('icon-star').addClass('icon-star-empty');
    	}else{
        	$(_this).find('i').removeClass('icon-star-empty').addClass('icon-star');
        	onceFocusObj[uid]=1;
    	}
	},444);
}*/
//提示
function sucFai(data) {
    var m='';
    if (data[0]||data){
        m='操作成功';
    }else {
        m='操作失败';
    }
    $('#pormpt p').text(m);
    $('#pormpt').modal('show');
	/*$('#pormpt').on('hidden.bs.modal', function (e) {
		var table_url = '';
  		if($('#myTabs li').eq(0).hasClass('active')){
			$('#influence').bootstrapTable('destroy');
			$('#influe p').show();
			var ytl=$('#inputList input:radio[name="xnr123"]:checked').attr('tp');
			//table_url = '/weibo_xnr_operate/related_recommendation/?xnr_user_no='+ID_Num+'&sort_item='+ytl;	
		}else if($('#myTabs li').eq(1).hasClass('active')){
			$('#influence2').bootstrapTable('destroy');
			$('#influe2 p').show();
			//table_url = '/weibo_xnr_operate/daily_recomment_tweets/?xnr_user_no='+ID_Num+'&sort_item=user_index';
		}
		//public_ajax.call_request('get',table_url,related);
	})*/
}




