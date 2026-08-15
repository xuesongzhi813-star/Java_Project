package org.example.messagequeuepromax.mqserver.core;


import org.example.messagequeuepromax.common.exchangeType;

/**
 * 对于不同交换机类型，不同转发规则方法的实现类
 */

public class Router {


    //规则：bindingKey：1.只能有数字，字母，下划线，“#”，“*”构成；2.由“.”分割；3.“#”，“*”都只能单独成部分
    public boolean checkBindingKey(String bindingKey){
        //空字串，对于DIRECT,FANOUT可行，DIRECT不需要bk，FANOUT绑定就能发送
        if(bindingKey==""){
            return true;
        }
        //1.遍历判断bk中单独每个字符，是否满足规则
        for (int i=0;i<bindingKey.length();i++){
            char s=bindingKey.charAt(i);
            //判断该字符是否是大写字母
            if(s>='A'&& s<='Z'){
                continue;
            }
            //判断该字符是否是小写字母
            else if (s>='a'&& s<='z') {
                continue;
            }
            //判断字母是否是数字
            else if (s>='0' && s<='9') {
                continue;
            }
            //判断字母是否是"_"/"."
            else if (s=='_' || s=='.' || s=='#' || s=='*') {
                continue;
            }
            else {
                //若都不是上述情况则，rk不合法
                return false;
            }
        }
        //2.判断“#”和“*”是否单独成一个部分，按照"."分割字符串先
        String[] split = bindingKey.split("\\.");
        for (int i=0;i<split.length;i++){
            //若长度>1且有“*”/“#”则不合法
            if(split[i].length()>1 && (split[i].contains("#") || split[i].contains("*"))){
                return false;
            }
        }
        //3.自定义规则的判断：
        //（1）.aaa.#.#.bbb：与aaa.#.bbb含义相同，设为不合法（减少无效代码）
        //（2）.aaa.#.*.bbb：与aaa.#.bbb含义相同，设为不合法
        //（3）.aaa.*.#.bbb：与aaa.#.bbb含义相同，设为不合法
        //（4）.aaa.*.*.bbb：与aaa.#.bbb含义相同，设为合法
        for (int i = 0; i <split.length-1; i++) {
            if(split[i].equals("#") && split[i+1].equals("#")){
                return false;
            } else if (split[i].equals("#") && split[i+1].equals("*")) {
                return false;
            } else if (split[i].equals("*") && split[i+1].equals("#")) {
                return false;
            }
        }
        return true;
    }

    //规则：routingKey：1.只能有数字，字母，下划线构成；2.由"."分割
    //例如:aaa.bb.cc
    public boolean chechRoutingKey(String routingKey) {
        //空字串，对于FANOUT类型交换机可行，全都要发一遍
        if(routingKey==""){
            return true;
        }
        //遍历判断rk中单独每个字符，是否满足规则
        for (int i=0;i<routingKey.length();i++){
            char s=routingKey.charAt(i);
            //判断该字符是否是大写字母
            if(s>='A'&& s<='Z'){
                continue;
            }
            //判断该字符是否是小写字母
            else if (s>='a'&& s<='z') {
                continue;
            }
            //判断字母是否是数字
            else if (s>='0' && s<='9') {
                continue;
            }
            //判断字母是否是"_"/"."
            else if (s=='_' || s=='.') {
                continue;
            }
            else {
                //若都不是上述情况则，rk不合法
                return false;
            }
        }
        return true;
    }

    /**
     * “*”只能替代任意单独一个部分
     * “#”可以替代0个或者多个单独部分
     * @param exchangeType
     * @param message
     * @param value
     * @return
     */
    public boolean BindkeyMatchRoutingkey(exchangeType exchangeType, Binding value, Message message) {
        //本方法只针对TOPIC的bindingKey与routingKey匹配，其他类型交换机无法作用
        if(exchangeType!= exchangeType.TOPIC){
            return false;
        }
        //先分割两个字符串，并且设置指针
        String[] split1 = message.getroutingKey().split("\\.");
        String[] split2 = value.getBindingKey().split("\\.");
        int routingIndex=0;
        int bindingIndex=0;
        //循环对比
        while (routingIndex<split1.length && bindingIndex<split2.length){
            //如果是bindkey此时指向“*”，则直接替代，进行下一轮比较
            if(split2[bindingIndex].equals("*")){
                routingIndex++;
                bindingIndex++;
                continue;
            }
            //如果bindingKey此时指向“#”，则要判断bk后面是否还有值，若无直接替代，若有，要在rk中找到对应的
            else if (split2[bindingIndex].equals("#") && bindingIndex==(split2.length-1) ){
                    //“#”后无值，直接替代rk后全部，直接返回
                    return true;
            }
            else if (split2[bindingIndex].equals("#") && bindingIndex+1!=split2.length) {
                    //“#”后有值，拿“#”后内容去rk中比较寻找
                routingIndex = findExists(split2, split1, bindingIndex, routingIndex);
                //如果没找到，则匹配失败
                if(routingIndex==-1){
                    return false;
                }
                //如果找到则继续向后寻找
                //指向rk现在指向的串
                bindingIndex++;
                //整体向后移动
                bindingIndex++;
                routingIndex++;
            }
                //指向的是普通字符串，要两者完全相同才行
            else {
                if(!split1[routingIndex].equals(split2[bindingIndex])){
                    return false;
                }
                //完全相同才能继续向后判断
                routingIndex++;
                bindingIndex++;
            }
        }
        //判断二者是否一同到达末尾，若某串还有剩余，则匹配失败
        if(routingIndex==split1.length && bindingIndex==split2.length){
            return true;
        }
        return false;
    }

    private int findExists(String[] split2, String[] split1, int bindingIndex, int routingIndex) {
        //先存下要去rk中寻找的目标串
        String order=split2[bindingIndex+1];
        //从当前位置开始（因为可能出现“#”指代空的情况），开始向后寻找
        for(int i=routingIndex;i<split1.length;i++){
            if(split1[i].equals(order)){
                return i;
            }
        }
        return -1;
    }

}
