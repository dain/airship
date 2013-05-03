package io.airlift.airship.agent;

public class Progress
{
    private String description;
    private Double progress;

    public synchronized void reset(String description)
    {
        this.description = description;
    }

    public synchronized String getDescription()
    {
        return description;
    }

    public synchronized void setDescription(String description)
    {
        this.description = description;
    }

    public synchronized Double getProgress()
    {
        return progress;
    }

    public synchronized void setProgress(Double progress)
    {
        this.progress = progress;
    }
}
