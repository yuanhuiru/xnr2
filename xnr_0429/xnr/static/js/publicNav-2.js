function createRandomItemStyle() {
    return {
        normal: {
            color: 'rgb(' + [
                Math.round(Math.random() * 128+127),
                Math.round(Math.random() * 128+127),
                Math.round(Math.random() * 128+127)
            ].join(',') + ')'
        }
    };
}
function getDaysBefore($time) {
    var a=new Date(new Date(new Date().setDate(new Date().getDate()-Number($time))).setHours(0,0,0,0));
    var b=Date.parse(a)/1000;
    return b;
}
//=============控制时间区间 30天=========

//=====================
var localTime=new Date();
Y= localTime.getFullYear()+'-';
M=(localTime.getMonth()+1<10?'0'+(localTime.getMonth()+1):localTime.getMonth()+1)+'-';
D=(localTime.getDate()-1<10?'0'+(localTime.getDate()):localTime.getDate());
var $_time=Y+M+D;
//当天零点的时间戳
function todayTimetamp() {
    var start=new Date();
    start.setHours(0);
    start.setMinutes(0);
    start.setSeconds(0);
    start.setMilliseconds(0);
    var todayStartTime=Date.parse(start)/1000;
    return todayStartTime;
}
//昨天时间到23:59
function yesterday() {
    var day = new Date();
    day.setTime(day.getTime()-24*60*60*1000);
    var s1 = day.getFullYear()+ "-" + (day.getMonth()+1) + "-" + day.getDate();
    var s2 = (Date.parse(new Date(new Date(new Date(s1).toLocaleDateString()).getTime()+24*60*60*1000-1)))/1000;
    return s2;
}
//时间戳转时间
function getLocalTime(nS) {
    return new Date(parseInt(nS) * 1000).toLocaleString().replace(/年|月/g, "-").replace(/日|上午|下午/g, " ");
}
//去除空格
function delSpace(str){
    var str=str.replace(/<\/?[^>]*>/gim,"");//去掉所有的html标记
    var result=str.replace(/(^\s+)|(\s+$)/g,"");//去掉前后空格
    return  result.replace(/\s/g,"");//去除文章中间空格
}
//判断字符串中某个字符出现的次数
function patch(re,s){ //参数1正则式，参数2字符串
    re=eval("/"+re+"/ig"); //不区分大小写，如须则去掉i,改为 re=eval_r("/"+re+"/g")
    var len = s.match(re).length;
    return len;
}
//判断空对象
function isEmptyObject(e) {
    var t;
    for (t in e)
        return !1;
    return !0
}
//判断在数组中是否有该值
function isInArray(arr,value){
    var index = $.inArray(value,arr);
    if(index >= 0){
        return true;
    }
    return false;
};
//删除数组指定项
Array.prototype.removeByValue = function(val) {
    for(var i=0; i<this.length; i++) {
        if(this[i] == val) {
            this.splice(i, 1);
            break;
        }
    }
};
//跳转到指定发帖微博  onclick="jumpWeiboThis(this)"
function jumpWeiboThis(_this){
	var uid= $(_this).parents('.center_rel').find('.uid').text();
	var mid= $(_this).parents('.center_rel').find('.mid').text();
	var weibo_post_url='/index/url_trans/?uid='+uid+'&mid='+mid;
    public_ajax.call_request('get',weibo_post_url,goWeioPost);
}
function goWeioPost(data){
	$('#pormpt p').text('如果无法跳转,请将浏览器拦截窗口的权限清除，右上方查看。');
    $('#pormpt').modal('show');
	setTimeout(function(){window.open(data)},200);
}
//监测词
function monWD(){
	var monitor_keywords_url='/weibo_xnr_manage/show_xnr_monitor_words/?xnr_user_no='+ID_Num;
    public_ajax.call_request('get',monitor_keywords_url,monitor_keywords);
}
setTimeout(function(){
	monWD();
},2000);
var word;
function monitor_keywords(data){
	word=data['monitor_keywords'];
	if(word==''){
		$(".wordsList").html('暂无监测词。');
	}else {
		var dataList=word.split(',');
		var str='';
		dataList.forEach(function(item,index){
			str+='<span class="wordYG"><b>'+item+'</b>&nbsp;<i class="icon icon-edit" title="编辑" onclick="editWORD(this)"></i></span>'
		});
		$(".wordsList").html(str);
	}
}
var oldVal;
function editWORD(_this){
	oldVal=$($(_this).prev('b')).text();
	$("#WORDedit .yuan").text(oldVal);
	$("#WORDedit").modal("show");
}
function wordCHANGE(){
	var _val=$("#WORDedit .xian").val();
	if(_val==''){
		$('#pormpt p').text('新的监测词不能为空。');
        $('#pormpt').modal('show');
		return false;
	}else {
		var word2=word.split(',');
		var _index=word2.indexOf(oldVal);
		word2.splice(_index,1,_val);		
		var new_change_keywords_url='/weibo_xnr_manage/update_xnr_monitor_words/?xnr_user_no='+ID_Num+
			'&new_monitor_keywords='+word2.join('，');
	    public_ajax.call_request('get',new_change_keywords_url,new_change_news);
	}	
}
function new_change_news(data){
	var _in=data['status'];
    var txt= _in=='ok'?'更改成功':'更改失败';
    $('#pormpt p').text(txt);
    $('#pormpt').modal('show');
	setTimeout(function(){
    	monWD();
	},700);
}
//判断邮箱
function checkEmail(str){
			var re=/^([a-zA-Z0-9]+[_|\_|\.]?)*[a-zA-Z0-9]+@([a-zA-Z0-9]+[_|\_|\.]?)*[a-zA-Z0-9]+\.[a-zA-Z]{2,3}$/
    		if(re.test(str)){
    			return true;
        		//console.log("邮箱正确");
    		}else{
    			return false;
        		//console.log("邮箱错误");
	}
}
//检验密码是否正确
//var check_password_url='/weibo_xnr_manage/verify_xnr_account/?xnr_user_no='+ID_Num;
setTimeout(function(){
	if(loadingType=='weibo'){
		var check_password_url='/weibo_xnr_manage/verify_xnr_account/?xnr_user_no='+ID_Num;
		public_ajax.call_request('get',check_password_url,checkPassword);
	}
},2000);
function checkPassword(data){
    if(data['account_info']==0){
        $('.infoError').html('<span style="color:#03a9f4;">账户正常运行中。</span>');
    }else {
        $('.infoError').html('<span style="color:red;">账户异常，为保证虚拟人正常工作，请检查您的账户和密码是否正确，并及时更改。</span>');
    }
}

//更改XNR密码
$("#changePaaword").on('click',function(){
	$('#changePaawordOpt').modal('show');
});
$("#sureChangePaaword").on('click',function(){
    var email=$('.passval1').val();
	var phone=$('.passval2').val();
	var newPW=$('.passval3').val();
	if(email==''||checkEmail(email)== false||phone==''||newPW==''){
		$('#pormpt p').text('请检查您要输入的邮箱和手机号码是否正确，并且不能为空。');
	    $('#pormpt').modal('show');
	}else {
		var change_password_url='/weibo_xnr_manage/modify_xnr_account/?xnr_user_no='+ID_Num+'&weibo_mail_account='+email+
		'&weibo_phone_account='+phone+'&Password='+newPW;
	    public_ajax.call_request('get',change_password_url,changePWinfor);
	}
});
function changePWinfor(data){
	var _in=data['status'];
	var txt= _in=='ok'?'密码更改成功':'密码更改失败';
	$('#pormpt p').text(txt);
    $('#pormpt').modal('show');
}
//跳转微博首页
function jumpWeibo(uid){
	window.open('https://weibo.com/u/'+uid);
}
//查看全文
function allWord(_this) {
    var a=$(_this).attr('data-all');
    if (a==0){
        $(_this).text('收起');
        $(_this).parent('.center_rel').find('.center_2').html($(_this).next().html());
        $(_this).attr('data-all','1');
    }else {
        $(_this).text('查看全文');
        $(_this).parent('.center_rel').find('.center_2').html($(_this).next().next().html());
        $(_this).attr('data-all','0');
    }
}
//翻译
function Check(s) {
    var str = s.replace(/%/g, "%25").replace(/\+/g, "%2B").replace(/\s/g, "+"); // % + \s 3
    str = str.replace(/-/g, "%2D").replace(/\*/g, "%2A").replace(/\//g, "%2F"); // - * / 4
    str = str.replace(/\&/g, "%26").replace(/!/g, "%21").replace(/\=/g, "%3D"); // & ! = 5
    str = str.replace(/\?/g, "%3F").replace(/:/g, "%3A").replace(/\|/g, "%7C"); // ? : | 6
    str = str.replace(/\,/g, "%2C").replace(/\./g, "%2E").replace(/#/g, "%23"); // , . # 7
    return str;
}
var translateWordThis='';
function translateWord(_this) {
    var t=$(_this).parents('.center_rel').find('.center_2').text();
    var txt=Check(t);
    translateWordThis=_this;
    var translate_url='/index/text_trans/?q='+txt;
    public_ajax.call_request('get',translate_url,transSUCCESS)
}
function transSUCCESS(data) {
    if (data.length==0){data='暂无翻译内容';}
    $(translateWordThis).parents('.center_rel').find('.tsWord').text(data);
    $(translateWordThis).parent().prev('._translate').show();
}
//私信该用户
function emailThis(_this) {
    $(_this).parents('.center_rel').find('.emailDown').show();
}
function letter(_this) {
    var uid = $(_this).parents('.center_rel').find('.uid').text();
    var txt = Check($(_this).prev().val());
    var post_url_letter='/twitter_xnr_operate/private_operate/?xnr_user_no='+ID_Num+'&uid='+uid+
        '&text='+txt;
    public_ajax.call_request('get',post_url_letter,operatSuccess)
}
var urlFirst_zpd='',mft_id,ft,first_url,reportWaring,reportInfo,
    reply_retweet='retweet_operate',reply_comment='comment_operate';
setTimeout(function () {
    ft=$('.nav_type').text();
    if(ft=='(微博)'){
        urlFirst_zpd='weibo_xnr_operate';mft_id='mid';reportWaring='weibo_xnr_warming_new';
        reply_retweet='reply_retweet',reply_comment='reply_comment';reportInfo='weibo_info';
        first_url='/weibo_xnr_monitor/new_addto_weibo_corpus/';
    }else if(ft=='(FaceBook)'){
        urlFirst_zpd='facebook_xnr_operate';mft_id='fid';reportWaring='facebook_xnr_warning';
        first_url='/facebook_xnr_monitor/addto_facebook_corpus/';reportInfo='fb_info';
    }else if(ft=='(twitter)'){
        urlFirst_zpd='twitter_xnr_operate';mft_id='tid';reportWaring='twitter_xnr_warning';
        first_url='/twitter_xnr_monitor/addto_facebook_corpus/';reportInfo='tw_info';
    }
},1000);
//retweet_operate   comment_operate   like_operate
//转发 分享  转推
var for_type,for_this;
function retweet(_this,type) {
    for_type=type;
    for_this=_this;
    $(_this).parents('.center_rel').find('.forwardingDown').show();
}
function forwardingBtn() {
    var txt = $(for_this).parents('.center_rel').find('.forwardingIput').val();
    //if (txt!=''){
        var MFT = $(for_this).parents('.center_rel').find('.'+mft_id).text();
        var forPost_url='/'+urlFirst_zpd+'/'+reply_retweet+'/?tweet_type='+for_type+'&xnr_user_no='+ID_Num+
            '&text='+txt+'&'+mft_id+'='+MFT;
        if (loadingType!='weibo'){
            var uid = $(for_this).parents('.center_rel').find('.uid').text();
            forPost_url+='&uid='+uid;
        }
        public_ajax.call_request('get',forPost_url,operatSuccess);
    //}else {
    //    $('#pormpt p').text('转发内容不能为空。');
    //    $('#pormpt').modal('show');
    //}
}
//评论
var for_this_pinglun;
function showInput(_this) {
   for_this_pinglun = _this;
    $(_this).parents('.center_rel').find('.commentDown').show();
};
function comMent(_this,type){
    var txt = Check($(_this).prev().val());
    if (txt!=''){
        var MFT = $(_this).parents('.center_rel').find('.'+mft_id).text();
        var comPost_url='/'+urlFirst_zpd+'/'+reply_comment+'/?tweet_type='+type+'&text='+txt+'&xnr_user_no='+
            ID_Num+'&'+mft_id+'='+MFT;
        if (loadingType!='weibo'){
            var uid = $(_this).parents('.center_rel').find('.uid').text();
            comPost_url+='&uid='+uid;
        }
        public_ajax.call_request('get',comPost_url,operatSuccess);
    }else {
        $('#pormpt p').text('评论内容不能为空。');
        $('#pormpt').modal('show');
    }
}
//关注用户
var focus_this_uid;
function focusUser(_this){
    focus_this_uid=$(_this).parents('.center_rel').find('.uid').text();
    $("#focusListType").modal('show');

//    var foc_url='/weibo_xnr_operate/follow_operate/?xnr_user_no='+ID_Num+'&uid='+uid;
  //  public_ajax.call_request('get',foc_url,focusYes);
}
$("#sureFlist").click(function(){
	 var _type=[];
    $("#focusListType input:checkbox:checked").each(function (index,item) {
        _type.push($(this).val());
    });
    if(_type.length==0){
        $('#pormpt p').text('请选择要关注的类型。');
        $('#pormpt').modal('show');
        return false;
    }
    var focus_thisuser_url='/weibo_xnr_operate/follow_operate/?xnr_user_no='+ID_Num+
            '&uid='+fs_uid_list.join('，')+'&follow_type='+_type.join('，');
    public_ajax.call_request('get',focus_thisuser_url,focusYes);

})
function focusYes(data){
    var _in=data['status'];
    var txt= _in=='ok'?'操作成功':'操作失败';
    $('#pormpt p').text(txt);
    $('#pormpt').modal('show');
}

/*function focusYes(data) {
    var f='';
    if (data[0]||data){
        f='操作成功';
    }else {
        f='操作失败';
    }
    $('#pormpt p').text(f);
    $('#pormpt').modal('show');
}*/
//点赞  喜欢
function thumbs(_this) {
    var MFT = $(_this).parents('.center_rel').find('.'+mft_id).text();
    var time=$(_this).parents('.center_rel').find('.timestamp').text();
    var likePost_url='/'+urlFirst_zpd+'/like_comment/?'+mft_id+'='+MFT+'&xnr_user_no='+ID_Num+'&timestamp='+time;
    if (loadingType=='faceBook'||loadingType=='twitter'){
        var uid = $(_this).parents('.center_rel').find('.uid').text();
        likePost_url+='&uid='+uid;
    }
    public_ajax.call_request('get',likePost_url,operatSuccess);
};
//机器人回复
var robotThis,robotTxt;
function robot(_this) {
    robotThis=_this;
    $(_this).parents('.center_rel').find('.commentDown').show();
    robotTxt= $(_this).parents('.center_rel').find('.center_2').text();
    //var robot_url='/facebook_xnr_operate/robot_reply/?question='+Check(txt);
    //public_ajax.call_request('get',robot_url,robotTxt);
	$('#RobotType').modal('show');
}
function sureRobotType(){
	//var robotTYPE=$('#RobotType input[name="rbt"]:checked').val();
	var robotTYPE=$("#RobotType input[name='rbt']:checked").val();
	if(robotTYPE=='1'){
		var robot_url='/facebook_xnr_operate/robot_reply/?question='+Check(robotTxt);
        public_ajax.call_request('get',robot_url,robotTxt1);
	}else {
		var robot2='/weibo_xnr_operate/network_buzzwords/';
		public_ajax.call_request('get',robot2,robotTxt2);
	}
}
function robotTxt2(data){
	$(robotThis).parents('.center_rel').find('.commentDown').children('input').val(data);
	$(robotThis).parents('.center_rel').find('.commentDown').show();
}
function robotTxt1(data) {
    $(robotThis).parents('.center_rel').find('.commentDown').show();
	$(robotThis).parents('.center_rel').find('.robotQuestion').remove();
    var txt=data['tuling'];
    if (isEmptyObject(data)||!txt){txt='机器人无答复'};
    $(robotThis).parents('.center_rel').find('.commentDown').children('input').val(txt);
    var robotType;
    try{
        robotType=$(robotThis).parents('.center_rel').find('.commentDown').children('span').attr('onclick').replace(/\(|\)|\'/g,'').split(',')[1];
    }catch (e){
        robotType=$(robotThis).parents('.center_rel').find('.commentDown').find('span').eq(1).attr('onclick').replace(/\(|\)|\'/g,'').split(',')[1];
    }
    /*var str='<div class="robotQuestion">';
    var problem=data["own"][0]||'没有相关问题';
    var robot1='<p style="font-weight: 900;color:#f6a38e;"><i class="icon icon-lightbulb"></i>&nbsp;相关问题</p>' +
        '<p style="text-indent:30px;margin:5px 0;">'+problem+'</p>';
    var robot2='<div><p style="font-weight: 900;color:#f6a38e;"><i class="icon icon-lightbulb"></i>&nbsp;相关评论</p>';
    var robot3='';
    if (data['own'][1]&&data['own'][1].length!=0){
        $.each(data['own'][1],function (index,item) {
            robot3+='<div class="robotDown" class="r"><input type="text" class="robotIput" value="'+item+'">&nbsp;' +
                '<span class="sureRobot" onclick="comMent(this,\''+robotType+'\')">回复</span></div>'
        });
    }else {
        robot3='<p style="text-indent:30px;margin:5px 0;">没有相关评论</p>';
    }
    robot2+=robot3+'</div>';
    str+=robot1+robot2+'</div>';
    $(robotThis).parents('.center_rel').find('.commentDown').after(str);*/
	$(robotThis).parents('.center_rel').find('.commentDown').show();
}
//加入预警库
function getInfo(_this) {
    var alldata=[];
    var uid = $(_this).parents('.center_rel').eq(0).find('.uid').text();alldata.push(uid);
    var mid = $(_this).parents('.center_rel').eq(0).find('.'+mft_id).text();alldata.push(mid);
    var timestamp = $(_this).parents('.center_rel').eq(0).find('.timestamp').text();alldata.push(timestamp);
    return alldata;
};
function joinPolice(_this,type) {
    var info=getInfo(_this);
    var police_url='/'+reportWaring+'/addto_warning_corpus/?xnr_user_no='+ID_Num+'&uid='+info[0]+
        '&'+mft_id+'='+info[1]+'&timestamp='+info[2]+'&warning_source='+type;
    public_ajax.call_request('get',police_url,operatSuccess)
};
//加入语料库
var wordUid,wordMid,wordTime;
function joinlab(_this) {
    wordMid = ($(_this).parents('.center_rel').find('.mid').text()||$(_this).parents('.center_rel').find('.fid').text()||
        $(_this).parents('.center_rel').find('.tid').text());
    wordUid = $(_this).parents('.center_rel').find('.uid').text();
    wordTime = $(_this).parents('.center_rel').find('.timestamp').text();
    $('#wordcloud').modal('show');
}
function joinWord() {
    var create_type=$('#wordcloud input:radio[name="xnr"]:checked').val();
    var corpus_type=$('#wordcloud input:radio[name="theday"]:checked').val();
    var theme_daily_name=[],tt=11;
    if (corpus_type=='主题语料'){tt=22};
    $("#wordcloud input:checkbox[name='theme"+tt+"']:checked").each(function (index,item) {
        theme_daily_name.push($(this).val());
    });
    var first_url,mftID;
    if(ft=='(微博)'){
        first_url='/weibo_xnr_monitor/new_addto_weibo_corpus/';mftID='mid';
    }else if(ft=='(FaceBook)'){
        first_url='/facebook_xnr_monitor/addto_facebook_corpus/';mftID='fid';
    }else if(ft=='(twitter)'){
        first_url='/twitter_xnr_monitor/addto_twitter_corpus/';mftID='tid';
    }
    var corpus_url= first_url+'?xnr_user_no='+ID_Num +
        '&corpus_type='+corpus_type+'&theme_daily_name='+theme_daily_name.join(',')+
        '&uid='+wordUid+'&'+mft_id+'='+wordMid+'&timestamp='+wordTime+'&create_type='+create_type;
    public_ajax.call_request('get',corpus_url,operatSuccess);
}
//一键上报
function oneUP(_this,type) {
    var len=$(_this).parents('.everyUser').find('.center_rel');
    if (len){
        var mainUID=$(_this).parents('.everyUser').find('.mainUID').text();
        var mainNAME=$(_this).parents('.everyUser').find('.centerNAME').text();
        var _id=$(_this).parents('.everyUser').find('._id').text();
        var dateTime='';
        var uidList=[],weibo_info=[];
        for (var i=0;i<len.length;i++){
            var uid=$(len[i]).find('.uid').text();uidList.push(uid);
            var mid = $(len[i]).find('.'+mft_id).text();
            var timestamp = $(len[i]).find('.timestamp').text();
            var a={};
            a[mft_id]=mid;a['timestamp']=timestamp;
            weibo_info.push(a);
			//weibo_info.push({'mid':mid,'timestamp':timestamp});
        }
        // var once_url='/weibo_xnr_warming/report_warming_content/?report_type='+type+'&xnr_user_no='+ID_Num+
        //     '&event_name='+mainNAME+'&uid='+mainUID+'&report_id='+_id+'&user_info='+uidList+
        //     '&weibo_info='+weibo_info;
        if (type=='人物'){
            mainNAME='';
        }else if (type=='言论'){
            mainNAME='';mainUID='';
        }else if (type=='事件'){
            mainUID='';
        }else if (type=='时间'){
            mainNAME='';mainUID='';
            dateTime=$(_this).parents('.everyUser').find('.timestamp').text();
        }
        var job={
            'report_type':type,
            'xnr_user_no':ID_Num,
            'report_id':_id,
            //=========
            'date_time':dateTime,
            'event_name':mainNAME,
            'uid':mainUID,
            'user_info':uidList,
            // 'weibo_info':weibo_info,
        }
		//job[mft_id]=mainUID;
        job[reportInfo]=weibo_info;
        $.ajax({
            type:'POST',
            url: '/'+reportWaring+'/report_warming_content/',
            contentType:"application/json",
            data: JSON.stringify(job),
            dataType: "json",
            success: operatSuccess,
        });
    }else {
        $('#pormpt p').text('微博内容为空，无法上报。');
        $('#pormpt').modal('show');
    }
}
//查看评论
function timeToYMD(nS) {
    let localTime=new Date(nS*1000);
    let Y= localTime.getFullYear()+'-';
    let M=(localTime.getMonth()+1<10?'0'+(localTime.getMonth()+1):localTime.getMonth()+1)+'-';
    let D=(localTime.getDate()<10?'0'+(localTime.getDate()):localTime.getDate());
    let $_time=Y+M+D;
    return $_time;
};
function commentList(_this){
	$(".commentTable").hide();
	$("#lookComment p.loading").show();
	$("#lookComment").modal('show');
	var t1= $(_this).parents('.center_rel').find('.timestamp').text();
	var time=timeToYMD(t1);
	var uid= $(_this).parents('.center_rel').find('.uid').text();
	var mid= $(_this).parents('.center_rel').find('.mid').text();
	var lookComment_url='/weibo_xnr_operate/select_weibo_comment/?tweet_time='+Check(time)+'&mid='+mid+'&uid='+uid;
	public_ajax.call_request('get',lookComment_url,showCommentList); 
}
function showCommentList(data){
	if(data.length==0){
		$("#lookComment p.loading").hide();
		$(".commentTable #commentTable").html('<p class="noData" style="font-size:18px;text-align:center;margin:20px 0;">暂无数据</p>');
		$(".commentTable").show();
		return false;
	}
	$('#commentTable').bootstrapTable('load', data);
$('#commentTable').bootstrapTable({
    data:data,
    search: true,//是否搜索
    pagination: true,//是否分页
    pageSize: 5,//单页记录数
    pageList: [2, 5, 10,20],//分页步进值
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
            title: "",//标题
            field: "",//键名
            sortable: true,//是否可排序
            order: "desc",//默认排序方式
            align: "center",//水平
            valign: "middle",//垂直
            formatter: function (value, row, index) {
                var name,time,txt,img;
                if (row.nick_name==''||row.nick_name=='null'||row.nick_name=='unknown'||!row.nick_name){
                    name=row.uid;
                }else {
                    name=row.nick_name;
                };
                if (row.timestamp==''||row.timestamp=='null'||row.timestamp=='unknown'){
                    time='未知';
                }else {
                    time=getLocalTime(row.timestamp);
                };
                if (row.photo_url==''||row.photo_url=='null'||row.photo_url=='unknown'||!row.photo_url){
                    img='/static/images/unknown.png';
                }else {
                    img=row.photo_url;
                };
                if (row.text==''||row.text=='null'||row.text=='unknown'){
                    txt='暂无内容';
                }else {
					txt=row.text;
                };
                var str=
                    '<div class="post_perfect" style="text-align:left;">'+
                    '   <div class="post_center-hot">'+
                    '       <img src="'+img+'" class="center_icon">'+
                    '       <div class="center_rel">'+
                    '           <a class="center_1" onclick="jumpWeiboThis(this)" style="color: #f98077;">'+name+'</a>'+
                    '           <span class="time" style="font-weight: 900;color:blanchedalmond;"><i class="icon icon-time"></i>&nbsp;&nbsp;'+time+'</span>  '+
				    '           <i class="mid" style="display: none;">'+row.mid+'</i>'+
                    '           <i class="uid" style="display: none;">'+row.uid+'</i>'+
					'           <i class="root_mid" style="display: none;">'+row.root_mid+'</i>'+
                    '           <i class="timestamp" style="display: none;">'+row.timestamp+'</i>'+
                    '           <span class="center_2" style="text-align: left;">'+txt+'</span>'+
					'           <div class="center_3">'+
					'               <span class="cen3-2" onclick="showInput(this)"><i class="icon icon-comments-alt"></i>&nbsp;&nbsp;回复</span>'+
					'               <span class="cen3-3" onclick="commentThumbs(this)"><i class="icon icon-thumbs-up"></i>&nbsp;&nbsp;赞</span>'+
                    '           </div>'+
                    '           <div class="commentDown" style="width: 100%;display: none;">'+
                    '               <input type="text" class="comtnt" placeholder="回复内容"/>'+
                    '               <span class="sureCom" onclick="replyComment(this)">回复</span>'+
                    '           </div>'+
                    '       </div>'+
                    '   </div>'+
                    '</div>';
                return str;
            }
        },
    ],
});
	$(".commentTable").show();
	$("#commentTable .noData").remove();
	$("#lookComment p.loading").hide();
}
function replyComment(_this){
	var uid = $(_this).parents('.center_rel').find('.uid').text();
	var mid = $(_this).parents('.center_rel').find('.mid').text();
	var rootid = $(_this).parents('.center_rel').find('.root_mid').text();
	var mid = $(_this).parents('.center_rel').find('.mid').text();
	var txt = $(_this).prev('.comtnt').val();
	var rep_com_url = '/weibo_xnr_operate/reply_total/?text='+txt+'&mid='+mid+'&r_mid='+rootid+'&uid='+uid+'&xnr_user_no='+ID_Num;
	public_ajax.call_request('get',rep_com_url,operatSuccess);
}
function commentThumbs(_this){
	var mid = $(_this).parents('.center_rel').find('.mid').text();
	var timestamp = $(_this).parents('.center_rel').find('.timestamp').text();
	var like_url='/weibo_xnr_operate/like_comment/?mid='+mid+'&xnr_user_no='+ID_Num+'&timestamp='+timestamp;
	public_ajax.call_request('get',like_url,operatSuccess);
}
//操作返回结果
function operatSuccess(data) {
    var f='';
    if (data[0]||data){f='操作成功';
		try{
			$(for_this).parents('.center_rel').find('.forwardingIput').val('');
        	$(for_this_pinglun).parents('.center_rel').find('.comtnt').val('');
		}catch(e){
		}
    }else {f='操作失败'};
    $('#pormpt p').text(f);
    $('#pormpt').modal('show');
}
