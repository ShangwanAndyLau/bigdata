package cn.andy;

/**
 * 单例延迟初始化：
 * 类的加载：当遇到调用静态变量的指令时，初始化该变量指向的类。
 *
 */
public class LazyLoadInstance {

    private LazyLoadInstance(){};

    private static  class LazyLoader{
        static  final LazyLoadInstance INSTANCE = new LazyLoadInstance();
    }

    public  static  final LazyLoadInstance getInstance(){
        return LazyLoader.INSTANCE;
    }
}
