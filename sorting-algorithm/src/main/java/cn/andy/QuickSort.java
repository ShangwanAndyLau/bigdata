package cn.andy;

public class QuickSort {

    // psvm是main函数的快捷键。
    public static void main(String[] args) {
        int[] arr = {5, 2, 9, 4, 7, 6, 1, 3, 8}; // 从小到大快速排序
        quickSort(arr, 0, 8);
        for (int i : arr) {
            System.out.print(i + ",");
        }
    }

    static void swap(int[] arr,int i,int j){
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    // 分区操作：快排的核心思想
    static int partition(int[] arr,int left,int right) {
        int pivot = arr[right]; // 这里每次都选择最后一个元素作为基准
        int tail = left - 1;    // tail为小于基准的子数组最后一个元素的索引
        // 遍历基准以外的元素
        for (int i = left; i < right; i++) {
            if (arr[i] < pivot) {
                swap(arr, ++tail, i);// 把小于等于基准的元素放到前一个子数组末尾
            }
        }

        swap(arr, tail + 1, right);// 最后把基准放到前一个子数组的后边，剩下的子数组既是大于基准的子数组
        // 该操作很有可能把后面元素的稳定性打乱，所以快速排序是不稳定的排序算法
        return tail + 1; // 返回基准的索引

    }

    static void quickSort(int[] arr,int left,int right){
        if (left >= right) {
            return;
        }

        int pivot_index = partition(arr, left, right);// 基准的索引
        quickSort(arr, left, pivot_index - 1);
        quickSort(arr, pivot_index + 1, right);
    }

}
