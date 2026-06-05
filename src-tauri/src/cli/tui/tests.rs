use std::sync::mpsc;
use std::{ffi::OsString, path::Path};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::{buffer::Buffer, layout::Rect};
use serde_json::json;
use serial_test::serial;
use tempfile::TempDir;

use super::app::{App, EditorSubmit, LoadingKind, Overlay, ToastKind};
use super::data::UiData;
use super::form::ProviderAddField;
use super::*;
use crate::cli::i18n::texts;
use crate::test_support::{
    lock_test_home_and_settings, set_test_home_override, TestHomeSettingsLock,
};
use crate::{AppError, AppType};

struct EnvGuard {
    _lock: TestHomeSettingsLock,
    old_home: Option<OsString>,
    old_userprofile: Option<OsString>,
}

impl EnvGuard {
    fn set_home(home: &Path) -> Self {
        let lock = lock_test_home_and_settings();
        let old_home = std::env::var_os("HOME");
        let old_userprofile = std::env::var_os("USERPROFILE");
        std::env::set_var("HOME", home);
        std::env::set_var("USERPROFILE", home);
        set_test_home_override(Some(home));
        crate::settings::reload_test_settings();
        Self {
            _lock: lock,
            old_home,
            old_userprofile,
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.old_home {
            Some(value) => std::env::set_var("HOME", value),
            None => std::env::remove_var("HOME"),
        }
        match &self.old_userprofile {
            Some(value) => std::env::set_var("USERPROFILE", value),
            None => std::env::remove_var("USERPROFILE"),
        }
        set_test_home_override(self.old_home.as_deref().map(Path::new));
        crate::settings::reload_test_settings();
    }
}

#[test]
fn mcp_import_uses_supported_apps_import_and_info_toast_kind() {
    let mut app = App::new(Some(AppType::OpenClaw));
    let mut data = UiData::default();

    import_mcp_from_supported_apps_with(
        &mut app,
        &mut data,
        || Ok(2),
        |_app_type| Ok(UiData::default()),
    )
    .expect("mcp import should work");

    let toast = app.toast.as_ref().expect("mcp import should show toast");
    assert_eq!(toast.kind, ToastKind::Info);
    assert_eq!(toast.message, texts::tui_toast_mcp_imported(2));
}

#[test]
fn tui_tick_rate_returns_to_200ms() {
    assert_eq!(TUI_TICK_RATE, std::time::Duration::from_millis(200));
}

#[test]
fn app_switch_cache_miss_queues_background_load_without_blocking() {
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    data.config.common_snippets.codex = Some("codex shared config".to_string());

    let mut cache = UiDataByAppCache::default();
    let (tx, rx) = mpsc::channel();

    cache
        .switch_to(&mut app, &mut data, Some(&tx), AppType::Codex)
        .expect("switch should not synchronously load app data");

    assert_eq!(app.app_type, AppType::Codex);
    assert!(data.providers.rows.is_empty());
    assert_eq!(data.config.common_snippet, "codex shared config");
    assert_eq!(
        cache.pending_by_app.get(&AppType::Codex).copied(),
        Some(PendingDataLoad {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
        })
    );
    assert!(cache.by_app.contains_key(&AppType::Claude));

    let req = rx.recv().expect("app data request should be queued");
    assert!(matches!(
        req,
        AppDataReq::Load {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Codex,
        }
    ));
}

#[test]
fn app_data_send_failure_does_not_block_retry() {
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    let (tx, rx) = mpsc::channel();
    drop(rx);

    cache
        .switch_to(&mut app, &mut data, Some(&tx), AppType::Codex)
        .expect("switch should still use loading projection on send failure");

    assert!(!cache.pending_by_app.contains_key(&AppType::Codex));
    assert!(cache.incomplete_by_app.contains(&AppType::Codex));

    let mut back_data = UiData::default();
    cache
        .switch_to(&mut app, &mut back_data, None, AppType::Claude)
        .expect("switch back should work");

    let (retry_tx, retry_rx) = mpsc::channel();
    cache
        .switch_to(&mut app, &mut back_data, Some(&retry_tx), AppType::Codex)
        .expect("retry switch should queue another load");

    assert_eq!(
        cache.pending_by_app.get(&AppType::Codex).copied(),
        Some(PendingDataLoad {
            request_id: 2,
            generation: 0,
            app_state_epoch: 0,
        })
    );
    assert!(matches!(
        retry_rx.recv().expect("retry should send request"),
        AppDataReq::Load {
            request_id: 2,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Codex,
        }
    ));
}

#[test]
fn stale_app_data_result_does_not_overwrite_current_app() {
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    data.providers.current_id = "claude-current".to_string();

    let mut cache = UiDataByAppCache::default();
    cache.pending_by_app.insert(
        AppType::Codex,
        PendingDataLoad {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
        },
    );

    let mut loaded = UiData::default();
    loaded.providers.current_id = "codex-loaded".to_string();

    handle_app_data_msg(
        &mut app,
        &mut data,
        &mut cache,
        None,
        AppDataMsg::Loaded {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Codex,
            result: Ok(loaded),
        },
    );

    assert_eq!(app.app_type, AppType::Claude);
    assert_eq!(data.providers.current_id, "claude-current");
    assert_eq!(
        cache
            .by_app
            .get(&AppType::Codex)
            .map(|cached| cached.providers.current_id.as_str()),
        Some("codex-loaded")
    );
}

#[test]
fn app_data_result_preserves_usage_pricing_that_finished_first() {
    let mut app = App::new(Some(AppType::Codex));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    cache.pending_by_app.insert(
        AppType::Codex,
        PendingDataLoad {
            request_id: 2,
            generation: 0,
            app_state_epoch: 0,
        },
    );
    cache.pending_usage_pricing_by_key.insert(
        (AppType::Codex, data::UsageRangePreset::SevenDays),
        PendingDataLoad {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
        },
    );

    let mut usage = data::UsageSnapshot::default();
    usage.summary_7d.total_cost_usd = 12.5;
    let pricing = data::ModelPricingSnapshot {
        rows: vec![data::ModelPricingRow {
            model_id: "gpt-5.4".to_string(),
            display_name: "GPT 5.4".to_string(),
            recent_total_cost_usd: 12.5,
            ..data::ModelPricingRow::default()
        }],
        ..data::ModelPricingSnapshot::default()
    };

    handle_usage_pricing_msg(
        &mut app,
        &mut data,
        &mut cache,
        UsagePricingMsg::Loaded {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Codex,
            range: data::UsageRangePreset::SevenDays,
            result: Ok(data::UsagePricingData {
                usage,
                pricing: Some(pricing),
            }),
        },
    );

    assert_eq!(data.usage.summary_7d.total_cost_usd, 12.5);
    assert!(
        !cache.by_app.contains_key(&AppType::Codex),
        "pending base data should not be cached as a complete app snapshot"
    );

    let mut loaded = UiData::default();
    loaded.providers.current_id = "codex-base".to_string();
    handle_app_data_msg(
        &mut app,
        &mut data,
        &mut cache,
        None,
        AppDataMsg::Loaded {
            request_id: 2,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Codex,
            result: Ok(loaded),
        },
    );

    assert_eq!(data.providers.current_id, "codex-base");
    assert_eq!(data.usage.summary_7d.total_cost_usd, 12.5);
    assert_eq!(data.pricing.rows.len(), 1);
}

#[test]
fn app_data_result_after_cache_invalidation_is_ignored() {
    let mut app = App::new(Some(AppType::Codex));
    let mut data = UiData::default();
    data.providers.current_id = "current-after-reload".to_string();
    let mut cache = UiDataByAppCache::default();
    cache.pending_by_app.insert(
        AppType::Codex,
        PendingDataLoad {
            request_id: 4,
            generation: 0,
            app_state_epoch: 0,
        },
    );

    cache.handle_data_reloaded(&app, &data, CacheInvalidation::DataReloaded);

    let mut loaded = UiData::default();
    loaded.providers.current_id = "stale-worker-result".to_string();
    handle_app_data_msg(
        &mut app,
        &mut data,
        &mut cache,
        None,
        AppDataMsg::Loaded {
            request_id: 4,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Codex,
            result: Ok(loaded),
        },
    );

    assert_eq!(data.providers.current_id, "current-after-reload");
    assert!(cache.pending_by_app.is_empty());
    assert_eq!(cache.data_generation, 1);
}

#[test]
fn no_op_reload_candidate_preserves_pending_app_data_load() {
    let mut terminal = TuiTerminal::new_for_test().expect("create terminal");
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    let mut proxy_loading = RequestTracker::default();
    let mut webdav_loading = RequestTracker::default();
    let mut update_check = RequestTracker::default();
    cache.pending_by_app.insert(
        AppType::Codex,
        PendingDataLoad {
            request_id: 7,
            generation: 0,
            app_state_epoch: 0,
        },
    );

    handle_tui_action(
        &mut terminal,
        &mut app,
        &mut data,
        &mut cache,
        None,
        None,
        None,
        None,
        None,
        &mut proxy_loading,
        None,
        None,
        None,
        &mut webdav_loading,
        None,
        &mut update_check,
        None,
        None,
        None,
        None,
        Action::EditorSubmit {
            submit: EditorSubmit::ProviderAdd,
            content: "{".to_string(),
        },
    )
    .expect("invalid submit should be handled as a no-op");

    assert_eq!(
        cache.pending_by_app.get(&AppType::Codex).copied(),
        Some(PendingDataLoad {
            request_id: 7,
            generation: 0,
            app_state_epoch: 0,
        })
    );
    assert_eq!(cache.data_generation, 0);
}

#[test]
fn usage_pricing_results_are_tracked_per_app() {
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    let (tx, rx) = mpsc::channel();

    cache.queue_usage_pricing_load(
        &mut app,
        Some(&tx),
        &AppType::Claude,
        data::UsageRangePreset::SevenDays,
    );
    cache.queue_usage_pricing_load(
        &mut app,
        Some(&tx),
        &AppType::Codex,
        data::UsageRangePreset::SevenDays,
    );

    let requests = [rx.recv().unwrap(), rx.recv().unwrap()];
    assert!(requests.iter().any(|req| matches!(
        req,
        UsagePricingReq::Load {
            request_id: 1,
            app_type: AppType::Claude,
            ..
        }
    )));
    assert!(requests.iter().any(|req| matches!(
        req,
        UsagePricingReq::Load {
            request_id: 2,
            app_type: AppType::Codex,
            ..
        }
    )));

    let mut claude_usage = data::UsageSnapshot::default();
    claude_usage.summary_7d.total_cost_usd = 1.0;
    let mut codex_usage = data::UsageSnapshot::default();
    codex_usage.summary_7d.total_cost_usd = 2.0;

    handle_usage_pricing_msg(
        &mut app,
        &mut data,
        &mut cache,
        UsagePricingMsg::Loaded {
            request_id: 2,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Codex,
            range: data::UsageRangePreset::SevenDays,
            result: Ok(data::UsagePricingData {
                usage: codex_usage,
                pricing: Some(data::ModelPricingSnapshot::default()),
            }),
        },
    );
    handle_usage_pricing_msg(
        &mut app,
        &mut data,
        &mut cache,
        UsagePricingMsg::Loaded {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Claude,
            range: data::UsageRangePreset::SevenDays,
            result: Ok(data::UsagePricingData {
                usage: claude_usage,
                pricing: Some(data::ModelPricingSnapshot::default()),
            }),
        },
    );

    assert_eq!(data.usage.summary_7d.total_cost_usd, 1.0);
    assert_eq!(
        cache
            .usage_pricing_by_key
            .get(&(AppType::Codex, data::UsageRangePreset::SevenDays))
            .map(|usage_pricing| usage_pricing.usage.summary_7d.total_cost_usd),
        Some(2.0)
    );
}

#[test]
fn usage_custom_range_action_queues_range_specific_load() {
    let mut terminal = TuiTerminal::new_for_test().expect("create terminal");
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    let mut proxy_loading = RequestTracker::default();
    let mut webdav_loading = RequestTracker::default();
    let mut update_check = RequestTracker::default();
    let (tx, rx) = mpsc::channel();
    let range =
        data::parse_usage_custom_range("2026-06-01..2026-06-05").expect("valid custom range");
    data.usage.recent_logs.push(data::UsageLogRow {
        request_id: "stale-log".to_string(),
        ..data::UsageLogRow::default()
    });
    data.usage.logs_total = 1;
    data.usage.recent_logs_custom.push(data::UsageLogRow {
        request_id: "stale-custom-log".to_string(),
        ..data::UsageLogRow::default()
    });
    data.usage.logs_total_custom = 1;

    handle_tui_action(
        &mut terminal,
        &mut app,
        &mut data,
        &mut cache,
        None,
        None,
        None,
        None,
        None,
        &mut proxy_loading,
        None,
        None,
        None,
        &mut webdav_loading,
        None,
        &mut update_check,
        None,
        None,
        None,
        Some(&tx),
        Action::UsageCustomRange { range },
    )
    .expect("custom range action should be handled");

    assert!(matches!(
        app.usage.range,
        data::UsageRangePreset::Custom(active) if active == range
    ));
    assert_eq!(data.usage.custom_range, Some(range));
    assert!(!data.usage.trends_custom.is_empty());
    assert_eq!(data.usage.recent_logs.len(), 1);
    assert_eq!(data.usage.logs_total, 1);
    assert!(data
        .usage
        .recent_logs_for(data::UsageRangePreset::Custom(range))
        .is_empty());
    assert_eq!(
        data.usage
            .logs_total_for(data::UsageRangePreset::Custom(range)),
        0
    );
    assert_eq!(
        cache
            .pending_usage_pricing_by_key
            .get(&(AppType::Claude, data::UsageRangePreset::Custom(range)))
            .copied(),
        Some(PendingDataLoad {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
        })
    );
    assert!(matches!(
        rx.recv().expect("custom usage/pricing request should be queued"),
        UsagePricingReq::Load {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Claude,
            range: data::UsageRangePreset::Custom(queued_range),
        } if queued_range == range
    ));
}

#[test]
fn usage_custom_range_app_switch_does_not_show_stale_custom_cache() {
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    let active_range =
        data::parse_usage_custom_range("2026-06-01..2026-06-05").expect("valid active range");
    let stale_range =
        data::parse_usage_custom_range("2026-05-01..2026-05-05").expect("valid stale range");

    app.usage.range = data::UsageRangePreset::Custom(active_range);
    data.usage.begin_custom_range(active_range);

    let mut stale_usage = data::UsageSnapshot::default();
    stale_usage.custom_range = Some(stale_range);
    stale_usage.summary_custom.total_requests = 99;
    stale_usage.summary_custom.total_cost_usd = 12.34;
    cache.usage_pricing_by_key.insert(
        (AppType::Codex, data::UsageRangePreset::Custom(stale_range)),
        data::UsagePricingData {
            usage: stale_usage,
            pricing: None,
        },
    );
    cache.by_app.insert(AppType::Codex, UiData::default());

    cache
        .switch_to(&mut app, &mut data, None, AppType::Codex)
        .expect("switch should work");

    assert_eq!(app.app_type, AppType::Codex);
    assert_eq!(data.usage.custom_range, Some(active_range));
    assert_eq!(data.usage.summary_custom.total_requests, 0);
    assert_eq!(data.usage.summary_custom.total_cost_usd, 0.0);
}

#[test]
fn usage_fixed_result_does_not_replace_active_custom_logs() {
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    let active_range =
        data::parse_usage_custom_range("2026-06-01..2026-06-05").expect("valid active range");
    app.usage.range = data::UsageRangePreset::Custom(active_range);
    data.usage.begin_custom_range(active_range);
    data.usage.recent_logs_custom.push(data::UsageLogRow {
        request_id: "custom-log".to_string(),
        ..data::UsageLogRow::default()
    });
    data.usage.logs_total_custom = 1;
    cache.pending_usage_pricing_by_key.insert(
        (AppType::Claude, data::UsageRangePreset::SevenDays),
        PendingDataLoad {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
        },
    );

    let mut fixed_usage = data::UsageSnapshot::default();
    fixed_usage.summary_7d.total_requests = 10;
    fixed_usage.recent_logs.push(data::UsageLogRow {
        request_id: "fixed-log".to_string(),
        ..data::UsageLogRow::default()
    });
    fixed_usage.logs_total = 10;

    handle_usage_pricing_msg(
        &mut app,
        &mut data,
        &mut cache,
        UsagePricingMsg::Loaded {
            request_id: 1,
            generation: 0,
            app_state_epoch: 0,
            app_type: AppType::Claude,
            range: data::UsageRangePreset::SevenDays,
            result: Ok(data::UsagePricingData {
                usage: fixed_usage,
                pricing: Some(data::ModelPricingSnapshot::default()),
            }),
        },
    );

    assert_eq!(data.usage.summary_7d.total_requests, 10);
    assert_eq!(
        data.usage
            .logs_total_for(data::UsageRangePreset::Custom(active_range)),
        1
    );
    assert_eq!(
        data.usage
            .recent_logs_for(data::UsageRangePreset::Custom(active_range))[0]
            .request_id,
        "custom-log"
    );
    assert_eq!(
        data.usage.logs_total_for(data::UsageRangePreset::SevenDays),
        10
    );
    assert_eq!(
        data.usage
            .recent_logs_for(data::UsageRangePreset::SevenDays)[0]
            .request_id,
        "fixed-log"
    );
}

#[test]
#[serial]
fn usage_custom_range_reload_requeues_active_custom_range() {
    let temp_home = TempDir::new().expect("create temp home");
    let _env = EnvGuard::set_home(temp_home.path());
    let mut terminal = TuiTerminal::new_for_test().expect("create terminal");
    let mut app = App::new(Some(AppType::Claude));
    let mut data = UiData::default();
    let mut cache = UiDataByAppCache::default();
    let mut proxy_loading = RequestTracker::default();
    let mut webdav_loading = RequestTracker::default();
    let mut update_check = RequestTracker::default();
    let (tx, rx) = mpsc::channel();
    let range =
        data::parse_usage_custom_range("2026-06-01..2026-06-05").expect("valid custom range");

    app.usage.range = data::UsageRangePreset::Custom(range);
    data.usage.custom_range = Some(range);
    data.usage.summary_custom.total_requests = 42;

    handle_tui_action(
        &mut terminal,
        &mut app,
        &mut data,
        &mut cache,
        None,
        None,
        None,
        None,
        None,
        &mut proxy_loading,
        None,
        None,
        None,
        &mut webdav_loading,
        None,
        &mut update_check,
        None,
        None,
        None,
        Some(&tx),
        Action::ReloadData,
    )
    .expect("reload data should be handled");

    assert_eq!(data.usage.custom_range, Some(range));
    assert_eq!(data.usage.summary_custom.total_requests, 0);
    assert!(matches!(
        rx.recv().expect("custom usage/pricing reload should be queued"),
        UsagePricingReq::Load {
            request_id: 1,
            app_type: AppType::Claude,
            range: data::UsageRangePreset::Custom(queued_range),
            ..
        } if queued_range == range
    ));
}

#[test]
fn skills_scan_unmanaged_uses_info_toast_kind() {
    let mut app = App::new(Some(AppType::OpenCode));

    scan_unmanaged_skills_with(&mut app, || Ok(Vec::new())).expect("skills scan should work");

    let toast = app.toast.as_ref().expect("skills scan should show toast");
    assert_eq!(toast.kind, ToastKind::Info);
    assert_eq!(toast.message, texts::tui_toast_unmanaged_scanned(0));
}

#[test]
fn opening_skills_import_picker_selects_all_by_default() {
    let mut app = App::new(Some(AppType::Claude));

    open_skills_import_picker_with(&mut app, || {
        Ok(vec![crate::services::skill::UnmanagedSkill {
            directory: "hello-skill".to_string(),
            name: "Hello Skill".to_string(),
            description: Some("A local skill".to_string()),
            found_in: vec!["claude".to_string()],
            path: "/tmp/hello-skill".to_string(),
        }])
    })
    .expect("import picker should open");

    assert!(matches!(
        &app.overlay,
        Overlay::SkillsImportPicker {
            skills,
            selected_idx: 0,
            selected,
        } if skills.len() == 1
            && skills[0].directory == "hello-skill"
            && selected.contains("hello-skill")
    ));
}

#[test]
fn skills_import_from_apps_uses_info_toast_kind() {
    let mut app = App::new(Some(AppType::OpenCode));
    let mut data = UiData::default();

    finish_skills_import_with(
        &mut app,
        &mut data,
        || Ok(vec![]),
        |_app_type| Ok(UiData::default()),
    )
    .expect("skills import should work");

    let toast = app.toast.as_ref().expect("skills import should show toast");
    assert_eq!(toast.kind, ToastKind::Info);
    assert_eq!(toast.message, texts::tui_toast_unmanaged_imported(0));
}

#[test]
fn proxy_help_overlay_uses_on_demand_proxy_config() {
    let mut app = App::new(Some(AppType::Claude));
    let data = UiData::default();

    open_proxy_help_overlay_with(&mut app, &data, || {
        Ok(Some(crate::proxy::ProxyConfig {
            listen_address: "127.0.0.1".to_string(),
            listen_port: 3456,
            ..crate::proxy::ProxyConfig::default()
        }))
    })
    .expect("proxy help overlay should open");

    let Overlay::TextView(view) = &app.overlay else {
        panic!("expected proxy help overlay");
    };
    let joined = view.lines.join("\n");
    assert!(joined.contains("cc-switch proxy serve --listen-address 127.0.0.1 --listen-port 3456"));
    assert!(joined.contains("ANTHROPIC_BASE_URL=http://127.0.0.1:3456"));
}

#[test]
fn managed_proxy_action_enqueues_background_request_and_shows_loading_overlay() {
    let mut app = App::new(Some(AppType::Claude));
    let mut loading = RequestTracker::default();
    let (tx, rx) = mpsc::channel();

    queue_managed_proxy_action(&mut app, Some(&tx), &mut loading, AppType::Claude, true)
        .expect("queue proxy action should succeed");

    let req = rx.recv().expect("proxy request should be queued");
    assert!(matches!(
        req,
        ProxyReq::SetManagedSessionForCurrentApp {
            request_id: 1,
            app_type: AppType::Claude,
            enabled: true,
        }
    ));
    assert_eq!(loading.active, Some(1));
    assert!(matches!(
        app.overlay,
        Overlay::Loading {
            kind: LoadingKind::Proxy,
            ..
        }
    ));
}

#[test]
fn proxy_open_flash_runner_persists_effect_across_frames() {
    let mut flash = ProxyOpenFlash::default();
    let mut app = App::new(Some(AppType::Claude));
    app.proxy_visual_transition = Some(super::app::ProxyVisualTransition {
        from_on: false,
        to_on: true,
        started_tick: 10,
    });
    let area = Rect::new(0, 0, 20, 2);

    flash.sync(&app, area);
    assert!(flash.active());

    let mut first = Buffer::empty(area);
    flash.process(std::time::Duration::from_millis(500), &mut first, area);
    assert!(flash.active(), "flash should still be active at peak frame");

    let mut second = Buffer::empty(area);
    flash.process(std::time::Duration::from_millis(100), &mut second, area);
    assert!(
        flash.active(),
        "flash should still be active during return phase"
    );
}

#[test]
fn managed_proxy_action_warns_when_worker_is_unavailable() {
    let mut app = App::new(Some(AppType::Claude));
    let mut loading = RequestTracker::default();

    queue_managed_proxy_action(&mut app, None, &mut loading, AppType::Claude, true)
        .expect("missing worker should not crash");

    let toast = app.toast.as_ref().expect("warning toast should be shown");
    assert_eq!(toast.kind, ToastKind::Warning);
    assert_eq!(
        toast.message,
        texts::tui_toast_proxy_request_failed(texts::tui_error_proxy_worker_unavailable())
    );
    assert!(matches!(app.overlay, Overlay::None));
    assert_eq!(loading.active, None);
}

#[test]
fn normalize_ctrl_h_becomes_backspace() {
    let key = KeyEvent::new_with_kind(
        KeyCode::Char('h'),
        KeyModifiers::CONTROL,
        KeyEventKind::Press,
    );
    let normalized = normalize_key_event(key);
    assert_eq!(normalized.code, KeyCode::Backspace);
    assert!(!normalized.modifiers.contains(KeyModifiers::CONTROL));
}

#[test]
fn normalize_plain_h_unchanged() {
    let key = KeyEvent::new_with_kind(KeyCode::Char('h'), KeyModifiers::NONE, KeyEventKind::Press);
    let normalized = normalize_key_event(key);
    assert_eq!(normalized.code, KeyCode::Char('h'));
    assert_eq!(normalized.modifiers, KeyModifiers::NONE);
}

#[test]
fn normalize_real_backspace_unchanged() {
    let key = KeyEvent::new_with_kind(KeyCode::Backspace, KeyModifiers::NONE, KeyEventKind::Press);
    let normalized = normalize_key_event(key);
    assert_eq!(normalized.code, KeyCode::Backspace);
}

#[test]
fn quick_setup_helper_saves_preset_and_runs_connection_check() {
    let mut captured = None;
    let mut checked = false;

    apply_webdav_jianguoyun_quick_setup(
        " demo@nutstore.com ",
        " app-password ",
        |cfg| {
            captured = Some(cfg);
            Ok(())
        },
        || {
            checked = true;
            Ok(())
        },
    )
    .expect("quick setup helper should succeed");

    let saved = captured.expect("settings should be saved");
    assert!(saved.enabled);
    assert_eq!(saved.base_url, "https://dav.jianguoyun.com/dav");
    assert_eq!(saved.remote_root, "cc-switch-sync");
    assert_eq!(saved.profile, "default");
    assert_eq!(saved.username, "demo@nutstore.com");
    assert_eq!(saved.password, "app-password");
    assert!(checked, "connection check should be called");
}

#[test]
fn quick_setup_helper_stops_when_save_fails() {
    let mut checked = false;
    let err = apply_webdav_jianguoyun_quick_setup(
        "u",
        "p",
        |_cfg| Err(AppError::Message("save failed".to_string())),
        || {
            checked = true;
            Ok(())
        },
    )
    .expect_err("save failure should be returned");

    assert!(err.to_string().contains("save failed"));
    assert!(!checked, "connection check should not run when save fails");
}

#[test]
fn stream_check_result_lines_include_core_fields() {
    let result = crate::services::stream_check::StreamCheckResult {
        status: crate::services::stream_check::HealthStatus::Degraded,
        success: true,
        message: "slow but working".to_string(),
        response_time_ms: Some(6789),
        http_status: Some(200),
        model_used: "gpt-5.1-codex".to_string(),
        tested_at: 1_700_000_000,
        retry_count: 1,
    };

    let lines = build_stream_check_result_lines("Provider One", &result);
    let joined = lines.join("\n");

    assert!(joined.contains("Provider One"));
    assert!(joined.contains("gpt-5.1-codex"));
    assert!(joined.contains("200"));
    assert!(joined.contains("6789"));
    assert!(joined.contains("slow but working"));
}

#[test]
fn external_editor_helper_replaces_editor_buffer_and_keeps_initial_text() {
    let mut app = App::new(Some(crate::AppType::Claude));
    app.open_editor(
        "Prompt",
        super::app::EditorKind::Plain,
        "hello",
        super::app::EditorSubmit::PromptEdit {
            id: "pr1".to_string(),
        },
    );

    run_external_editor_for_current_editor(&mut app, |current| {
        assert_eq!(current, "hello");
        Ok("hello from external\neditor".to_string())
    })
    .expect("external editor helper should succeed");

    let editor = app.editor.as_ref().expect("editor should stay open");
    assert_eq!(editor.text(), "hello from external\neditor");
    assert_eq!(editor.initial_text, "hello");
    assert!(editor.is_dirty(), "updated buffer should remain unsaved");
}

#[test]
fn external_editor_helper_preserves_buffer_on_error() {
    let mut app = App::new(Some(crate::AppType::Claude));
    app.open_editor(
        "Prompt",
        super::app::EditorKind::Plain,
        "hello",
        super::app::EditorSubmit::PromptEdit {
            id: "pr1".to_string(),
        },
    );

    let err = run_external_editor_for_current_editor(&mut app, |_current| {
        Err(AppError::Message("boom".to_string()))
    })
    .expect_err("external editor helper should surface the edit error");

    assert!(err.to_string().contains("boom"));
    let editor = app.editor.as_ref().expect("editor should stay open");
    assert_eq!(editor.text(), "hello");
    assert_eq!(editor.initial_text, "hello");
    assert!(
        !editor.is_dirty(),
        "failed external edit must not dirty the buffer"
    );
}

#[test]
fn drain_latest_webdav_req_prefers_last_enqueued_request() {
    let (tx, rx) = mpsc::channel();
    tx.send(WebDavReq {
        request_id: 1,
        kind: WebDavReqKind::CheckConnection,
    })
    .expect("send check request");
    tx.send(WebDavReq {
        request_id: 2,
        kind: WebDavReqKind::Upload,
    })
    .expect("send upload request");
    tx.send(WebDavReq {
        request_id: 3,
        kind: WebDavReqKind::JianguoyunQuickSetup {
            username: "u@example.com".to_string(),
            password: "p".to_string(),
        },
    })
    .expect("send quick setup request");

    let first = rx.recv().expect("receive first request");
    let latest = drain_latest_webdav_req(first, &rx);
    assert!(matches!(
        latest,
        WebDavReq {
            request_id: 3,
            kind: WebDavReqKind::JianguoyunQuickSetup { username, password }
        }
            if username == "u@example.com" && password == "p"
    ));
}

#[test]
fn update_webdav_last_error_with_updates_status_when_present() {
    let mut captured = None;
    update_webdav_last_error_with(
        Some("network timeout".to_string()),
        || Some(crate::settings::WebDavSyncSettings::default()),
        |cfg| {
            captured = Some(cfg);
            Ok(())
        },
    );

    let saved = captured.expect("expected settings to be saved");
    assert_eq!(saved.status.last_error.as_deref(), Some("network timeout"));
}

#[test]
fn update_webdav_last_error_with_skips_when_settings_absent() {
    let mut saved = false;
    update_webdav_last_error_with(
        Some("network timeout".to_string()),
        || None,
        |_cfg| {
            saved = true;
            Ok(())
        },
    );
    assert!(
        !saved,
        "set callback should not run when webdav settings are missing"
    );
}

#[test]
fn update_success_does_not_force_exit_when_overlay_hidden() {
    let mut app = App::new(None);
    app.overlay = Overlay::None;
    let mut update_check = RequestTracker::default();

    handle_update_msg(
        &mut app,
        &mut update_check,
        UpdateMsg::DownloadFinished(Ok("v9.9.9".to_string())),
    );

    assert!(
        !app.should_quit,
        "successful update should not force exit without user confirmation"
    );
    assert!(
        matches!(app.overlay, Overlay::UpdateResult { success: true, .. }),
        "successful update should show result overlay even when progress overlay was hidden"
    );
}

#[test]
fn update_check_finished_is_ignored_when_canceled() {
    let mut app = App::new(None);
    app.overlay = Overlay::None;
    let mut update_check = RequestTracker::default();

    let info = crate::cli::commands::update::UpdateCheckInfo {
        current_version: "4.7.0".to_string(),
        target_tag: "v9.9.9".to_string(),
        is_already_latest: false,
        is_downgrade: false,
        is_homebrew_managed: false,
    };

    handle_update_msg(
        &mut app,
        &mut update_check,
        UpdateMsg::CheckFinished {
            request_id: 1,
            result: Ok(info),
        },
    );

    assert!(
        matches!(app.overlay, Overlay::None),
        "update check result should be ignored after cancel/hide"
    );
}

#[test]
fn update_check_finished_is_processed_when_request_id_matches() {
    let mut app = App::new(None);
    app.overlay = Overlay::Loading {
        kind: LoadingKind::UpdateCheck,
        title: texts::tui_update_checking_title().to_string(),
        message: texts::tui_loading().to_string(),
    };
    let mut update_check = RequestTracker::default();
    update_check.active = Some(7);

    let info = crate::cli::commands::update::UpdateCheckInfo {
        current_version: "4.7.0".to_string(),
        target_tag: "v9.9.9".to_string(),
        is_already_latest: false,
        is_downgrade: false,
        is_homebrew_managed: false,
    };

    handle_update_msg(
        &mut app,
        &mut update_check,
        UpdateMsg::CheckFinished {
            request_id: 7,
            result: Ok(info),
        },
    );

    assert_eq!(update_check.active, None);
    assert!(matches!(
        app.overlay,
        Overlay::UpdateAvailable {
            latest,
            selected: 0,
            ..
        } if latest == "v9.9.9"
    ));
}

#[test]
fn update_check_finished_for_homebrew_update_shows_brew_toast() {
    let mut app = App::new(None);
    app.overlay = Overlay::Loading {
        kind: LoadingKind::UpdateCheck,
        title: texts::tui_update_checking_title().to_string(),
        message: texts::tui_loading().to_string(),
    };
    let mut update_check = RequestTracker::default();
    update_check.active = Some(7);

    let info = crate::cli::commands::update::UpdateCheckInfo {
        current_version: "4.7.0".to_string(),
        target_tag: "v9.9.9".to_string(),
        is_already_latest: false,
        is_downgrade: false,
        is_homebrew_managed: true,
    };

    handle_update_msg(
        &mut app,
        &mut update_check,
        UpdateMsg::CheckFinished {
            request_id: 7,
            result: Ok(info),
        },
    );

    assert_eq!(update_check.active, None);
    assert!(matches!(app.overlay, Overlay::None));
    let toast = app.toast.as_ref().expect("homebrew update should toast");
    assert_eq!(toast.kind, ToastKind::Info);
    assert!(toast.message.contains("v9.9.9"));
    assert!(toast.message.contains("brew upgrade cc-switch"));
}

#[test]
fn update_check_finished_is_ignored_when_request_id_mismatch() {
    let mut app = App::new(None);
    app.overlay = Overlay::None;
    let mut update_check = RequestTracker::default();
    update_check.active = Some(2);

    let stale = crate::cli::commands::update::UpdateCheckInfo {
        current_version: "4.7.0".to_string(),
        target_tag: "v1.0.0".to_string(),
        is_already_latest: false,
        is_downgrade: false,
        is_homebrew_managed: false,
    };
    handle_update_msg(
        &mut app,
        &mut update_check,
        UpdateMsg::CheckFinished {
            request_id: 1,
            result: Ok(stale),
        },
    );

    assert_eq!(update_check.active, Some(2));
    assert!(matches!(app.overlay, Overlay::None));

    let latest = crate::cli::commands::update::UpdateCheckInfo {
        current_version: "4.7.0".to_string(),
        target_tag: "v9.9.9".to_string(),
        is_already_latest: false,
        is_downgrade: false,
        is_homebrew_managed: false,
    };
    handle_update_msg(
        &mut app,
        &mut update_check,
        UpdateMsg::CheckFinished {
            request_id: 2,
            result: Ok(latest),
        },
    );

    assert_eq!(update_check.active, None);
    assert!(matches!(app.overlay, Overlay::UpdateAvailable { .. }));
}

#[test]
fn model_fetch_strategy_matches_provider_field() {
    assert_eq!(
        model_fetch_strategy_for_field(ProviderAddField::CodexModel),
        ModelFetchStrategy::Bearer
    );
    assert_eq!(
        model_fetch_strategy_for_field(ProviderAddField::GeminiModel),
        ModelFetchStrategy::GoogleApiKey
    );
    assert_eq!(
        model_fetch_strategy_for_field(ProviderAddField::ClaudeModelConfig),
        ModelFetchStrategy::Anthropic
    );
    assert_eq!(
        model_fetch_strategy_for_field(ProviderAddField::HermesModels),
        ModelFetchStrategy::Bearer
    );
}

#[test]
fn model_fetch_candidate_urls_prefers_v1_for_anthropic_base() {
    let urls = build_model_fetch_candidate_urls(
        "https://api.anthropic.com",
        ModelFetchStrategy::Anthropic,
    );
    assert_eq!(
        urls,
        vec![
            "https://api.anthropic.com/v1/models".to_string(),
            "https://api.anthropic.com/models".to_string()
        ]
    );
}

#[test]
fn model_fetch_candidate_urls_strip_anthropic_compat_suffix() {
    let urls = build_model_fetch_candidate_urls(
        "https://api.deepseek.com/anthropic",
        ModelFetchStrategy::Anthropic,
    );
    assert_eq!(
        urls,
        vec![
            "https://api.deepseek.com/anthropic/v1/models".to_string(),
            "https://api.deepseek.com/v1/models".to_string(),
            "https://api.deepseek.com/models".to_string(),
        ]
    );
}

#[test]
fn model_fetch_candidate_urls_for_gemini_v1beta_keeps_models_endpoint() {
    let urls = build_model_fetch_candidate_urls(
        "https://generativelanguage.googleapis.com/v1beta",
        ModelFetchStrategy::GoogleApiKey,
    );
    assert_eq!(
        urls,
        vec!["https://generativelanguage.googleapis.com/v1beta/models".to_string()]
    );
}

#[test]
#[serial(home_settings)]
fn startup_hidden_requested_app_bootstrap_uses_visible_app_normalization_before_loading_data() {
    let temp_home = TempDir::new().expect("create temp home");
    let _env = EnvGuard::set_home(temp_home.path());
    crate::settings::set_visible_apps(crate::settings::VisibleApps {
        claude: true,
        codex: true,
        gemini: false,
        opencode: true,
        hermes: false,
        openclaw: true,
    })
    .expect("save visible apps");

    let mut loaded_app_type = None;
    let (app, _data) = initialize_app_state_for_test(Some(AppType::Gemini), |app_type| {
        loaded_app_type = Some(app_type.clone());
        Ok(UiData::default())
    })
    .expect("bootstrap app state");

    assert_eq!(loaded_app_type, Some(AppType::OpenCode));
    assert_eq!(app.app_type, AppType::OpenCode);
}

#[test]
#[serial(home_settings)]
fn startup_reads_persisted_common_config_notice_confirmation() {
    let temp_home = TempDir::new().expect("create temp home");
    let _env = EnvGuard::set_home(temp_home.path());
    crate::settings::set_common_config_confirmed(true).expect("save confirmation");

    let (app, _data) =
        initialize_app_state_for_test(Some(AppType::Claude), |_| Ok(UiData::default()))
            .expect("bootstrap app state");

    assert!(app.common_config_notice_confirmed);
}

#[test]
fn parse_model_ids_supports_multiple_shapes_and_dedups_stably() {
    let data_payload = json!({
        "data": [
            {"id": "gpt-4o"},
            {"id": "gpt-4o-mini"},
            {"id": "gpt-4o"},
            {"id": "o3"}
        ]
    });
    assert_eq!(
        parse_model_ids_from_response(&data_payload),
        vec!["gpt-4o", "gpt-4o-mini", "o3"]
    );

    let gemini_payload = json!({
        "models": [
            {"name": "models/gemini-2.0-pro"},
            {"name": "models/gemini-2.0-flash"}
        ]
    });
    assert_eq!(
        parse_model_ids_from_response(&gemini_payload),
        vec!["gemini-2.0-pro", "gemini-2.0-flash"]
    );
}
