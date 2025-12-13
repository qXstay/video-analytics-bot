CREATE TABLE IF NOT EXISTS videos (
    id UUID PRIMARY KEY,
    creator_id TEXT NOT NULL,
    video_created_at TIMESTAMPTZ NOT NULL,

    views_count INTEGER NOT NULL,
    likes_count INTEGER NOT NULL,
    comments_count INTEGER NOT NULL,
    reports_count INTEGER NOT NULL,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id UUID PRIMARY KEY,
    video_id UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,

    views_count INTEGER NOT NULL,
    likes_count INTEGER NOT NULL,
    comments_count INTEGER NOT NULL,
    reports_count INTEGER NOT NULL,

    delta_views_count INTEGER NOT NULL,
    delta_likes_count INTEGER NOT NULL,
    delta_comments_count INTEGER NOT NULL,
    delta_reports_count INTEGER NOT NULL,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);