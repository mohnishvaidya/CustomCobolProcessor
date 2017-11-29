package com.example.stage;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  ERCODE1("A configuration is invalid because: {}"),
  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}