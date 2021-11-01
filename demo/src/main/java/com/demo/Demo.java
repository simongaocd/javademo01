package com.demo;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;
public class Demo {
   public static void main(String[] args) {
      LocalDateTime dateTime = LocalDateTime.now();
      System.out.println("DateTime = "+dateTime);
      String str = dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      System.out.println("Formatted date = "+str);
   }
}