package org.apache.hadoop.explorer.yarn.model;

import java.util.List;

/**
 * Result of turning a set of queue drafts into edits of capacity-scheduler.xml.
 * A plan with errors must never be deployed.
 */
public record PatchPlan(List<ConfigPatchOp> operations, List<String> errors, List<BranchBalance> balances) {

    public boolean isValid() {
        return errors.isEmpty();
    }
}
