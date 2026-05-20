'use client';

import { useTranslations } from 'next-intl';
import { Info, Tag, Github } from 'lucide-react';
import { APP_VERSION, GITHUB_REPO } from '@/lib/info';
import { useNowVersion } from '@/api/endpoints/update';

export function SettingInfo() {
    const t = useTranslations('setting');
    const nowVersionQuery = useNowVersion();

    const backendNowVersion = nowVersionQuery.data || '';

    return (
        <div className="rounded-3xl border border-border bg-card p-6 space-y-5">
            <h2 className="text-lg font-bold text-card-foreground flex items-center gap-2">
                <Info className="h-5 w-5" />
                {t('info.title')}
            </h2>
            {/* GitHub 仓库 */}
            <div className="flex items-center justify-between gap-4">
                <div className="flex items-center gap-3">
                    <Github className="h-5 w-5 text-muted-foreground" />
                    <span className="text-sm font-medium">{t('info.github')}</span>
                </div>
                <a
                    href={GITHUB_REPO}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sm text-primary hover:underline"
                >
                    {GITHUB_REPO.replace('https://github.com/', '')}
                </a>
            </div>
            {/* 当前版本 */}
            <div className="flex items-center justify-between gap-4">
                <div className="flex items-center gap-3">
                    <Tag className="h-5 w-5 text-muted-foreground" />
                    <span className="text-sm font-medium">{t('info.currentVersion')}</span>
                </div>
                <code className="text-sm font-mono text-muted-foreground">
                    {backendNowVersion || t('info.unknown')}
                </code>
            </div>
        </div>
    );
}
