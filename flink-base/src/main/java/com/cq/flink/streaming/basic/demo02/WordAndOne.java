package com.cq.flink.streaming.basic.demo02;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author chenquan
 * @Description 自定义结果对象,用于存储 word -> 1
 * @Date 2022-04-16 1:05
 **/

@AllArgsConstructor
@NoArgsConstructor
@Data
public class WordAndOne {
    private String word;
    private Integer one;

}
