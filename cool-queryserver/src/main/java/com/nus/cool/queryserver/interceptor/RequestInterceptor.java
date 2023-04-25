package com.nus.cool.queryserver.interceptor;


import com.nus.cool.queryserver.utils.Util;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * RequestInterceptor.
 */
@Component
public class RequestInterceptor implements HandlerInterceptor {

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    // Your pre-processing code goes here
    System.out.println("version 0.1");
    Util.getTimeClock();

    return true;
  }

}

