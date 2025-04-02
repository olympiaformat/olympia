/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.format.olympia;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.format.olympia.proto.objects.IsolationLevel;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.tree.TreeRoot;
import org.format.olympia.util.ValidationUtil;

/**
 * Representation of a transaction object.
 *
 * <p>It is expected that this object continue to be updated during operations. This object is not
 * thread safe.
 */
public class Transaction implements Serializable {

  private String transactionId;

  private TreeRoot beginningRoot;

  private TreeRoot runningRoot;

  private long beganAtMillis;

  private long expireAtMillis;

  private IsolationLevel isolationLevel;

  private boolean committed;

  private List<Action> actions;

  private Transaction() {}

  public static Builder builder() {
    return new Builder();
  }

  String transactionId() {
    return transactionId;
  }

  public void setTransactionId(String transactionId) {
    ValidationUtil.checkState(!committed, "Transaction is already committed");
    this.transactionId = transactionId;
  }

  TreeRoot beginningRoot() {
    return beginningRoot;
  }

  public void setBeginningRoot(TreeRoot beginningRoot) {
    ValidationUtil.checkState(!committed, "Transaction is already committed");
    this.beginningRoot = beginningRoot;
  }

  TreeRoot runningRoot() {
    return runningRoot;
  }

  public void setRunningRoot(TreeRoot runningRoot) {
    ValidationUtil.checkState(!committed, "Transaction is already committed");
    this.runningRoot = runningRoot;
  }

  long beganAtMillis() {
    return beganAtMillis;
  }

  public void setBeganAtMillis(long beganAtMillis) {
    ValidationUtil.checkState(!committed, "Transaction is already committed");
    this.beganAtMillis = beganAtMillis;
  }

  long expireAtMillis() {
    return expireAtMillis;
  }

  public void setExpireAtMillis(long expireAtMillis) {
    ValidationUtil.checkState(!committed, "Transaction is already committed");
    this.expireAtMillis = expireAtMillis;
  }

  IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  public void setIsolationLevel(IsolationLevel isolationLevel) {
    ValidationUtil.checkState(!committed, "Transaction is already committed");
    this.isolationLevel = isolationLevel;
  }

  public boolean isCommitted() {
    return committed;
  }

  public void setCommitted() {
    ValidationUtil.checkState(!committed, "Transaction is already committed");
    this.committed = true;
  }

  public List<Action> actions() {
    return actions;
  }

  public void addAction(Action action) {
    this.actions.add(action);
  }

  public void setActions(Collection<Action> actions) {
    this.actions = Lists.newArrayList(actions);
  }

  public static class Builder {

    private String transactionId;

    private TreeRoot beginningRoot;

    private TreeRoot runningRoot;

    private Long beganAtMillis;

    private Long expireAtMillis;

    private IsolationLevel isolationLevel;

    private boolean committed = false;

    private List<Action> actions = Lists.newArrayList();

    public Builder() {}

    public Builder setTransactionId(String transactionId) {
      this.transactionId = transactionId;
      return this;
    }

    public Builder setBeginningRoot(TreeRoot beginningRoot) {
      this.beginningRoot = beginningRoot;
      return this;
    }

    public Builder setRunningRoot(TreeRoot runningRoot) {
      this.runningRoot = runningRoot;
      return this;
    }

    public Builder setBeganAtMillis(long beganAtMillis) {
      this.beganAtMillis = beganAtMillis;
      return this;
    }

    public Builder setExpireAtMillis(long expireAtMillis) {
      this.expireAtMillis = expireAtMillis;
      return this;
    }

    public Builder setIsolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
      return this;
    }

    public Builder setCommitted() {
      this.committed = true;
      return this;
    }

    public Builder addAction(Action action) {
      this.actions.add(action);
      return this;
    }

    public Builder setActions(Collection<Action> actions) {
      this.actions = Lists.newArrayList(actions);
      return this;
    }

    public Transaction build() {
      Transaction transaction = new Transaction();

      if (transactionId != null) {
        transaction.setTransactionId(transactionId);
      } else {
        transaction.setTransactionId(UUID.randomUUID().toString());
      }

      ValidationUtil.checkNotNull(beginningRoot, "beginning root must be provided");
      transaction.setBeginningRoot(beginningRoot);

      ValidationUtil.checkNotNull(runningRoot, "running root must be provided");
      transaction.setRunningRoot(runningRoot);

      ValidationUtil.checkNotNull(beganAtMillis, "began timestamp must be provided");
      transaction.setBeganAtMillis(beganAtMillis);

      ValidationUtil.checkNotNull(expireAtMillis, "expire timestamp must be provided");
      transaction.setExpireAtMillis(expireAtMillis);

      ValidationUtil.checkNotNull(isolationLevel, "isolation level must be provided");
      transaction.setIsolationLevel(isolationLevel);

      if (committed) {
        transaction.setCommitted();
      }

      transaction.setActions(actions);

      return transaction;
    }
  }
}
