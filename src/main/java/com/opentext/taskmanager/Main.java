package com.opentext.taskmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {

        TaskExecutorService taskExecutor = new TaskExecutorService(10);
        TaskGroup taskGroup1 = new TaskGroup(UUID.randomUUID());
        TaskGroup taskGroup2 = new TaskGroup(UUID.randomUUID());
        List<Future<String>> answers = new ArrayList<>();
        for( int i =0 ;i<20 ;i++) {
            Task<String> t;
            if(i%2 ==0){
                 t= new Task(UUID.randomUUID(),taskGroup1,
                        TaskType.READ,new CTask1(i,taskGroup1));
            }else{
                t= new Task(UUID.randomUUID(),taskGroup2,
                        TaskType.WRITE,new CTask1(i,taskGroup2));
            }
            Future<String> answer = taskExecutor.submitTask(t); // sumbmit tasks
            answers.add(answer);
        }

        for (Future<String> ans: answers) {
            try {
                String answer = ans.get(1000, TimeUnit.SECONDS);
                //System.out.println(answer);
            }catch (InterruptedException | ExecutionException | TimeoutException e){
                System.out.println(e);
                taskExecutor.shutdown();
            }
        }
        taskExecutor.shutdown();
    }
}


class CTask1 implements Callable<String>{

    private int number;

    private TaskGroup taskGroup;
    CTask1( int number , TaskGroup taskGroup){
        this.number = number;
        this.taskGroup = taskGroup;
    }
    @Override
    public String call() {
        String taskNumber =null;
        try {
            if(number % 2 ==0 ){
                System.out.println("Even Thread="+Thread.currentThread().getName()+" currently Processing Task number= "+number + " in task group = "+taskGroup);
                //Thread.sleep(2000);
            }else{
                System.out.println(" Odd Thread="+Thread.currentThread().getName()+" currently Processing Task number= "+number + " in task group = "+taskGroup);
                Thread.sleep(1000);
            }
            taskNumber = String.format("Task Number = %s completed",number);
            return taskNumber;
        }catch (InterruptedException e){
            System.out.println(e);
        }
        return taskNumber;
    }
}
