package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class DraftValidateResponse {
    @JsonProperty("is_valid")
    private boolean valid;

    private List<BranchBalance> balances = new ArrayList<>();
    private List<String> errors = new ArrayList<>();
    private List<String> warnings = new ArrayList<>();

    public DraftValidateResponse() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final DraftValidateResponse obj = new DraftValidateResponse();

        public Builder valid(boolean valid) { obj.valid = valid; return this; }
        public Builder balances(List<BranchBalance> balances) { obj.balances = balances; return this; }
        public Builder errors(List<String> errors) { obj.errors = errors; return this; }
        public Builder warnings(List<String> warnings) { obj.warnings = warnings; return this; }

        public DraftValidateResponse build() { return obj; }
    }

    public boolean isValid() { return valid; }
    public void setValid(boolean valid) { this.valid = valid; }
    public List<BranchBalance> getBalances() { return balances; }
    public void setBalances(List<BranchBalance> balances) { this.balances = balances; }
    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
    public List<String> getWarnings() { return warnings; }
    public void setWarnings(List<String> warnings) { this.warnings = warnings; }
}
