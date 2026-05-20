'use client';

import { useStatsChannelPeriod, type StatsChannelPeriod } from '@/api/endpoints/stats';
import { useMemo } from 'react';
import { useTranslations } from 'next-intl';
import { TrendingUp, Gauge } from 'lucide-react';
import { Tabs, TabsList, TabsTrigger, TabsContents, TabsContent } from '@/components/animate-ui/components/animate/tabs';
import { useHomeViewStore, type RankSortMode, type RankPeriod } from '@/components/modules/home/store';
import { formatMoney, formatCount } from '@/lib/utils';

function getMedalEmoji(rank: number): string {
    switch (rank) {
        case 1: return '🥇';
        case 2: return '🥈';
        case 3: return '🥉';
        default: return '';
    }
}

export function Rank() {
    const t = useTranslations('home.rank');
    const rankSortMode = useHomeViewStore((s) => s.rankSortMode);
    const setRankSortMode = useHomeViewStore((s) => s.setRankSortMode);
    const rankPeriod = useHomeViewStore((s) => s.rankPeriod);
    const setRankPeriod = useHomeViewStore((s) => s.setRankPeriod);

    const { data: channels } = useStatsChannelPeriod(rankPeriod);

    const sorted = useMemo<StatsChannelPeriod[]>(() => {
        if (!channels) return [];
        const arr = [...channels];
        switch (rankSortMode) {
            case 'cost': arr.sort((a, b) => (b.total_cost || 0) - (a.total_cost || 0)); break;
            case 'count': arr.sort((a, b) => b.requests - a.requests); break;
            case 'tokens': arr.sort((a, b) => b.total_token - a.total_token); break;
            case 'speed': arr.sort((a, b) => b.tokens_per_sec - a.tokens_per_sec); break;
        }
        return arr;
    }, [channels, rankSortMode]);

    const renderList = (items: StatsChannelPeriod[]) => {
        if (items.length === 0) {
            return (
                <div className="flex flex-col items-center justify-center py-8 text-muted-foreground">
                    <TrendingUp className="w-12 h-12 mb-3 opacity-30" />
                    <p className="text-sm">{t('noData')}</p>
                </div>
            );
        }
        return (
            <div className="space-y-2 max-h-[340px] overflow-y-auto">
                {items.map((ch, idx) => {
                    const rank = idx + 1;
                    const medal = getMedalEmoji(rank);
                    return (
                        <div key={ch.channel_id} className="flex items-center gap-3 p-2.5 rounded-2xl hover:bg-accent/5 transition-colors">
                            <div className="w-7 h-7 rounded-lg flex items-center justify-center font-bold text-sm shrink-0">
                                {medal || rank}
                            </div>
                            <div className="flex-1 min-w-0">
                                <p className="font-medium text-sm truncate">{ch.channel_name}</p>
                                {rankSortMode === 'speed' && (
                                    <p className="text-xs text-muted-foreground mt-0.5">
                                        {formatCount(ch.output_token).formatted.value}{formatCount(ch.output_token).formatted.unit} / {ch.avg_latency}ms
                                    </p>
                                )}
                            </div>
                            <div className="text-right shrink-0">
                                {rankSortMode === 'cost' && (
                                    <span className="font-semibold text-sm">
                                        {formatMoney(ch.total_cost).formatted.value}
                                        <span className="text-xs text-muted-foreground ml-0.5">{formatMoney(ch.total_cost).formatted.unit}</span>
                                    </span>
                                )}
                                {rankSortMode === 'count' && (
                                    <span className="font-semibold text-sm">{ch.requests.toLocaleString()}</span>
                                )}
                                {rankSortMode === 'tokens' && (
                                    <span className="font-semibold text-sm">
                                        {formatCount(ch.total_token).formatted.value}
                                        <span className="text-xs text-muted-foreground ml-0.5">{formatCount(ch.total_token).formatted.unit}</span>
                                    </span>
                                )}
                                {rankSortMode === 'speed' && (
                                    <span className="font-semibold text-sm tabular-nums">
                                        {(ch.tokens_per_sec || 0).toFixed(1)}
                                        <span className="text-xs text-muted-foreground ml-0.5">tok/s</span>
                                    </span>
                                )}
                            </div>
                        </div>
                    );
                })}
            </div>
        );
    };

    const PERIODS: readonly { key: RankPeriod; label: string }[] = [
        { key: 'today', label: t('periodToday') },
        { key: '7d', label: t('period7d') },
        { key: '30d', label: t('period30d') },
    ];

    return (
        <div className="rounded-3xl bg-card text-card-foreground border-card-border border p-4">
            <Tabs value={rankSortMode} onValueChange={(v) => setRankSortMode(v as RankSortMode)}>
                <div className="flex items-center justify-between gap-2 mb-2">
                    <h3 className="font-semibold text-base shrink-0">{t('title')}</h3>
                    <div className="flex items-center gap-2 overflow-x-auto">
                        <select
                            value={rankPeriod}
                            onChange={(e) => setRankPeriod(e.target.value as RankPeriod)}
                            className="text-xs rounded-xl border border-border bg-muted/50 px-2 py-1 text-muted-foreground focus:outline-none"
                        >
                            {PERIODS.map((p) => (
                                <option key={p.key} value={p.key}>{p.label}</option>
                            ))}
                        </select>
                        <TabsList>
                            <TabsTrigger value="cost">{t('sortByCost')}</TabsTrigger>
                            <TabsTrigger value="count">{t('sortByCount')}</TabsTrigger>
                            <TabsTrigger value="tokens">{t('sortByTokens')}</TabsTrigger>
                            <TabsTrigger value="speed">{t('sortBySpeed')}</TabsTrigger>
                        </TabsList>
                    </div>
                </div>
                <TabsContents>
                    {(['cost', 'count', 'tokens', 'speed'] as RankSortMode[]).map((mode) => (
                        <TabsContent key={mode} value={mode}>
                            {renderList(sorted)}
                        </TabsContent>
                    ))}
                </TabsContents>
            </Tabs>
        </div>
    );
}
