use crate::cli::i18n::texts;
use crate::error::AppError;
use crate::services::PromptService;

use super::super::app::ToastKind;
use super::super::data::{load_state, UiData};
use super::helpers::{run_external_editor_for_prompt_form_content, select_prompt_by_id};
use super::RuntimeActionContext;

pub(super) fn activate(ctx: &mut RuntimeActionContext<'_>, id: String) -> Result<(), AppError> {
    let state = load_state()?;
    PromptService::enable_prompt(&state, ctx.app.app_type.clone(), &id)?;
    ctx.app
        .push_toast(texts::tui_toast_prompt_activated(), ToastKind::Success);
    *ctx.data = UiData::load(&ctx.app.app_type)?;
    Ok(())
}

pub(super) fn deactivate(ctx: &mut RuntimeActionContext<'_>, id: String) -> Result<(), AppError> {
    let state = load_state()?;
    PromptService::disable_prompt(&state, ctx.app.app_type.clone(), &id)?;
    ctx.app
        .push_toast(texts::tui_toast_prompt_deactivated(), ToastKind::Success);
    *ctx.data = UiData::load(&ctx.app.app_type)?;
    Ok(())
}

pub(super) fn update_metadata(
    ctx: &mut RuntimeActionContext<'_>,
    old_id: String,
    new_id: String,
    name: String,
    description: Option<String>,
) -> Result<(), AppError> {
    let state = load_state()?;
    let prompt = PromptService::update_prompt_metadata(
        &state,
        ctx.app.app_type.clone(),
        &old_id,
        &new_id,
        &name,
        description,
    )?;
    ctx.app.form = None;
    ctx.app
        .push_toast(texts::tui_toast_prompt_renamed(), ToastKind::Success);
    *ctx.data = UiData::load(&ctx.app.app_type)?;
    select_prompt_by_id(ctx.app, ctx.data, &prompt.id);
    Ok(())
}

pub(super) fn save(
    ctx: &mut RuntimeActionContext<'_>,
    old_id: Option<String>,
    new_id: String,
    name: String,
    description: Option<String>,
    content: String,
) -> Result<(), AppError> {
    let state = load_state()?;
    let prompt = match old_id {
        Some(old_id) => PromptService::update_prompt(
            &state,
            ctx.app.app_type.clone(),
            &old_id,
            &new_id,
            &name,
            description,
            Some(content),
        )?,
        None => PromptService::create_prompt_with_id(
            &state,
            ctx.app.app_type.clone(),
            Some(&new_id),
            &name,
            description.as_deref(),
            &content,
        )?,
    };

    ctx.app.form = None;
    ctx.app.editor = None;
    ctx.app
        .push_toast(texts::tui_toast_prompt_edit_finished(), ToastKind::Success);
    *ctx.data = UiData::load(&ctx.app.app_type)?;
    select_prompt_by_id(ctx.app, ctx.data, &prompt.id);
    Ok(())
}

pub(super) fn open_form_external(ctx: &mut RuntimeActionContext<'_>) -> Result<(), AppError> {
    ctx.terminal.with_terminal_restored(|| {
        run_external_editor_for_prompt_form_content(
            ctx.app,
            crate::cli::editor::open_external_editor,
        )
    })
}

pub(super) fn delete(ctx: &mut RuntimeActionContext<'_>, id: String) -> Result<(), AppError> {
    let state = load_state()?;
    PromptService::delete_prompt(&state, ctx.app.app_type.clone(), &id)?;
    ctx.app
        .push_toast(texts::tui_toast_prompt_deleted(), ToastKind::Success);
    *ctx.data = UiData::load(&ctx.app.app_type)?;
    Ok(())
}
