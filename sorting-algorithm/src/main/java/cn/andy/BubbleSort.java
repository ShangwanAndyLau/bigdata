package cn.andy;

public class BubbleSort {


     public static void main(String[] args){
         int[] arr = { 6, 5, 3, 1, 8, 7, 2, 4 };    // 从小到大冒泡排序

         // sout 输出System.out.println();的快捷键，fori是输出for循环的快捷键
         // 一键格式化代码：ctrl+alt+l
         for(int i : bubbleSort(arr)){
             System.out.print(i+",");
         }
     }

     static void swap(int[] arr,int i,int j){
         int tmp = arr[i];
         arr[i] = arr[j];
         arr[j] = tmp;
     }

     static int[] bubbleSort(int[] arr) {
         for (int i = 0; i < arr.length - 1; i++) {// 外循环控制排序躺数
             for (int j = 0; j < arr.length - 1 - i; j++) {// 内循环控制每一趟排序多少次
                 if (arr[j] > arr[j + 1]) {
                     swap(arr, j, j + 1);
                 }
             }
         }

         return arr;

     }
}
