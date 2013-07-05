package io.airlift.airship.cli;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.airship.shared.Assignment;
import io.airlift.airship.shared.SlotLifecycleState;
import io.airlift.airship.shared.SlotStatus;
import org.fusesource.jansi.Ansi.Color;

import static io.airlift.airship.cli.Ansi.colorize;

public class SlotRecord implements Record
{

    public static ImmutableList<Record> toSlotRecords(Iterable<SlotStatus> slots)
    {
        return ImmutableList.copyOf(Iterables.transform(slots, new Function<SlotStatus, Record>()
        {
            @Override
            public SlotRecord apply(SlotStatus slot)
            {
                return new SlotRecord(slot);
            }
        }));
    }

    private final SlotStatus slotStatus;

    public SlotRecord(SlotStatus statusRepresentation)
    {
        this.slotStatus = statusRepresentation;
    }

    public String getObjectValue(Column column)
    {
        Assignment assignment = slotStatus.getAssignment();
        Assignment expectedAssignment = slotStatus.getExpectedAssignment();
        switch (column) {
            case shortId:
                return slotStatus.getShortId();
            case uuid:
                return slotStatus.getId().toString();
            case machine:
                return slotStatus.getInstanceId();
            case internalHost:
                return slotStatus.getInternalHost();
            case internalIp:
                return slotStatus.getInternalIp();
            case externalHost:
                return slotStatus.getExternalHost();
            case status:
                return slotStatus.getState() == null ? null : slotStatus.getState().toString();
            case location:
                return slotStatus.getLocation();
            case shortLocation:
                return slotStatus.getShortLocation();
            case binary:
                return assignment == null ? null : assignment.getBinary();
            case shortBinary:
                return assignment == null ? null : assignment.getShortBinary();
            case config:
                return assignment == null ? null : assignment.getConfig();
            case shortConfig:
                return assignment == null ? null : assignment.getShortConfig();
            case expectedStatus:
                return slotStatus.getExpectedState() == null ? null : slotStatus.getExpectedState().toString();
            case expectedBinary:
                return expectedAssignment == null ? null : expectedAssignment.getBinary();
            case expectedConfig:
                return expectedAssignment == null ? null : expectedAssignment.getConfig();
            case statusMessage:
                return slotStatus.getStatusMessage();
            default:
                return null;
        }
    }

    @Override
    public String getValue(Column column)
    {
        return toString(getObjectValue(column));
    }

    @Override
    public String getColorizedValue(Column column)
    {
        Object value = getObjectValue(column);
        if (Column.status == column) {
            SlotLifecycleState state = SlotLifecycleState.lookup(toString(value));
            if (SlotLifecycleState.RUNNING == state) {
                return colorize(state, Color.GREEN);
            } else if (SlotLifecycleState.UNKNOWN == state) {
                return colorize(state, Color.RED);
            }
        } else if (Column.statusMessage == column) {
            return colorize(value, Color.RED);
        }
        return toString(value);
    }

    private String toString(Object value)
    {
        if (value == null) {
            return "";
        }
        return String.valueOf(value);
    }
}
