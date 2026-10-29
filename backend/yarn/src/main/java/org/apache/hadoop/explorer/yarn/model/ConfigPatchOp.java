package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One edit of capacity-scheduler.xml. The Ansible role applies the list of edits to the file
 * that is actually deployed on the ResourceManagers, so properties the explorer does not manage
 * (ACLs, global limits, resource calculator, ...) are never rewritten.
 *
 * <ul>
 *   <li>{@code set} — create or overwrite property {@code name} with {@code value};</li>
 *   <li>{@code remove} — delete property {@code name};</li>
 *   <li>{@code remove_prefix} — delete every property whose name starts with {@code name};</li>
 *   <li>{@code list_add} / {@code list_remove} — add/remove {@code value} in the comma-separated
 *       list stored in {@code name} (used for {@code <parent>.queues}).</li>
 * </ul>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConfigPatchOp(String op, String name, String value) {

    public static final String SET = "set";
    public static final String REMOVE = "remove";
    public static final String REMOVE_PREFIX = "remove_prefix";
    public static final String LIST_ADD = "list_add";
    public static final String LIST_REMOVE = "list_remove";

    public static ConfigPatchOp set(String name, String value) {
        return new ConfigPatchOp(SET, name, value);
    }

    public static ConfigPatchOp remove(String name) {
        return new ConfigPatchOp(REMOVE, name, null);
    }

    public static ConfigPatchOp removePrefix(String prefix) {
        return new ConfigPatchOp(REMOVE_PREFIX, prefix, null);
    }

    public static ConfigPatchOp listAdd(String name, String value) {
        return new ConfigPatchOp(LIST_ADD, name, value);
    }

    public static ConfigPatchOp listRemove(String name, String value) {
        return new ConfigPatchOp(LIST_REMOVE, name, value);
    }
}
