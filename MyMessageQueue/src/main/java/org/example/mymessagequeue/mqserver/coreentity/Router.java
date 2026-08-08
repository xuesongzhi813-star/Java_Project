package org.example.mymessagequeue.mqserver.coreentity;

public class Router {
    /**
     * 1.判断bindingKey的合法性
     * 2.判断routingKey的合法性
     */
    public static boolean bindingRouter(String bindingKey){
        //检查是否为空字符串
        if(bindingKey==""){
            //对于DIRECT交换机，routingKey就是队列名，不需要bindingKey
            //对于FANOUT交换机，所有队列都要发，不需要管bindingKey
            return true;
        }
        //判断bindingKey中字符是否合法
        //判断routingKey中字符是否合法
        for (int i = 0; i <bindingKey.length(); i++) {
            //从字符串中每次取一个‘字符’，判断是否是合法的，一直循环到结束
            char key=bindingKey.charAt(i);
            //判断是否是大写字母
            if(key>='A'&&key<='Z'){
                continue;
            }
            //判断是否是小写字母
            if(key>='a'&&key<='z'){
                continue;
            }
            //判断是否是数字
            if(key>='0'&&key<='9'){
                continue;
            }
            //判断是否是下划线/./#/*
            if (key == '_' || key == '.' || key == '#' || key == '*') {
                continue;
            }
            //全都不符合则，routingKey不合法
            return false;
        }
        //判断其中的#，*是否独立为一个单元
        String[] splits = bindingKey.split("//.");
        for (int i = 0; i <splits.length; i++) {
            //如果分割的单元，某块长度>1，且含有#/*中一个则不合法
            if(splits[i].length()>1 && (splits[i].contains("#") || splits[i].contains("*"))){
                return false;
            }
        }
        //自定义规则的判断：
        //1.aaa.#.#.bbb：与aaa.#.bbb含义相同，设为不合法（减少无效代码）
        //2.aaa.#.*.bbb：与aaa.#.bbb含义相同，设为不合法
        //3.aaa.*.#.bbb：与aaa.#.bbb含义相同，设为不合法
        //4.aaa.*.*.bbb：与aaa.#.bbb含义相同，设为合法
        for (int i = 0; i <splits.length-1; i++) {
            if(splits[i].equals("#") && splits[i+1].equals("#")){
                return false;
            } else if (splits[i].equals("#") && splits[i+1].equals("*")) {
                return false;
            } else if (splits[i].equals("*") && splits[i+1].equals("#")) {
                return false;
            }
        }
        return true;
    }

    public static boolean routingKeyRouter(String routingKey){
        //检查是否为空字符串
        if(routingKey==""){
            //对于FANOUT交换机，谁都会发送，不用匹配
            return true;
        }
        //判断routingKey中字符是否合法
        for (int i = 0; i <routingKey.length(); i++) {
            //从字符串中每次取一个‘字符’，判断是否是合法的，一直循环到结束
            char key=routingKey.charAt(i);
            //判断是否是大写字母
            if(key>='A'&&key<='Z'){
                continue;
            }
            //判断是否是小写字母
            if(key>='a'&&key<='z'){
                continue;
            }
            //判断是否是数字
            if(key>='0'&&key<='9'){
                continue;
            }
            //判断是否是下划线/.
            if (key == '_' || key == '.') {
                continue;
            }
            //全都不符合则，routingKey不合法
            return false;
        }
        return true;
    }

    /**
     *TOPIC交换机的转发规则：routingKey与bindingKey匹配
     * 1.routingKey与bindingKey匹配的判断
     * 2.辅助判断方法
     */
    // [测试用例]
    // binding key          routing key         result
    // 1.aaa                  aaa                 true
    // 2.aaa.bbb              aaa.bbb             true
    // 3.aaa.bbb              aaa.bbb.ccc         false
    // 4.aaa.bbb              aaa.ccc             false
    // 5.aaa.bbb.ccc          aaa.bbb.ccc         true
    // 6.aaa.*                aaa.bbb             true
    // 7.aaa.*.bbb            aaa.bbb.ccc         false
    // 8. *.aaa.bbb            aaa.bbb             false
    // 9. #                    aaa.bbb.ccc         true
    // 10.aaa.#                aaa.bbb             true
    // 11.aaa.#                aaa.bbb.ccc         true
    // 12.aaa.#.ccc            aaa.ccc             true
    // 13.aaa.#.ccc            aaa.bbb.ccc         true
    // 14.aaa.#.ccc            aaa.aaa.bbb.ccc     true
    // 15. #.ccc                ccc                 true
    // 16. #.ccc                aaa.bbb.ccc         true
    public static boolean routeTopic(exchangetype exchageType, Binding binding, Message message) {
        //若不是TOPIC交换机则不进行接下来的字符串匹配工作
        if(exchageType!=exchangetype.TOPIC){
            return false;
        }
        //先按'.'划分开来，并且分别设置下标
        String routingKey= message.getRoutingKey();
        String bindingKey= binding.getBindingKey();
        String[] routingKeyToken = routingKey.split("\\.");
        String[] bindingKeyToken = bindingKey.split("\\.");
        int routingIndex=0;
        int bindingIndex=0;
        //循环判断字符串是否匹配
        while (routingIndex<routingKeyToken.length && bindingIndex<bindingKeyToken.length){
            //若bindingKey此时指向'*'，直接后移即可
            if(bindingKeyToken[bindingIndex].equals("*")){
                routingIndex++;
                bindingIndex++;
                continue;
            }
            //若bindingKey此时指向'#'，且后面再无内容，直接返回true
            else if (bindingKeyToken[bindingIndex].equals("#") && (bindingIndex+1)==bindingKeyToken.length) {
                return true;
            }
            //若bindingKey此时指向'#'，且后面还有内容，需要拿bk'#'后内容去rk中比较
            else if(bindingKeyToken[bindingIndex].equals("#") && (bindingIndex+1)!=bindingKeyToken.length){
                bindingIndex++;
                routingIndex=findNextMatch(routingKeyToken,routingIndex,bindingKeyToken,bindingIndex);
                //如果routingKey中没有对'#'后内容匹配的部分，直接报错不合法
                if(routingIndex==-1){
                    return false;
                }
                //如果有内容匹配，则继续向后
                bindingIndex++;
                routingIndex++;
            }
            //指向普通字符串，内容必须完全匹配
            else {
                if(!bindingKeyToken[bindingIndex].equals(routingKeyToken[routingIndex])){

                    return false;
                }
                routingIndex++;
                bindingIndex++;
            }
        }
        //循环结束，判断是否两个字符串的下标，都同时到达了末尾
        if(bindingIndex==bindingKeyToken.length && routingIndex==routingKeyToken.length){
            return true;
        }
        return false;
    }

    //判断routingKey是否有字符串与"#"后字符串匹配
    private static int findNextMatch(String[] routingKeyToken, int routingIndex, String[] bindingKeyToken, int bindingIndex) {
        for (int i =routingIndex; i <routingKeyToken.length; i++) {
            if (routingKeyToken[i].equals(bindingKeyToken[bindingIndex])){
                return i;
            }
        }
        return -1;
    }
}
