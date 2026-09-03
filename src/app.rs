use std::fmt::Display;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::thread;
use std::time::Instant;

use anyhow::{Context as _, Result, anyhow};
use chrono::Local;
use egui::{
    Button, Color32, Context, DragValue, Id, Key, KeyboardShortcut, Modal, Modifiers, OpenUrl,
    PointerButton, RichText, Sense, ViewportCommand,
};
use egui_file_dialog::FileDialog;
use egui_notify::Toasts;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};

use crate::capture_health::CaptureHealth;
use crate::monitor::Monitor;
use crate::player_data::ExportSettings;
use crate::update::check_for_app_update;
use crate::{
    AppState, ConfirmationType, Message, ReloadHandle, State, TracingLevel, admin, capture,
    open_log_dir, wish,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SavedAppState {
    export_settings: ExportSettings,
    #[serde(default)]
    auto_start_capture: bool,
    log_raw_packets: bool,
    #[serde(default)]
    tracing_level: TracingLevel,
    /// Persisted capture backend override.  `None` means "use the command-line
    /// value / platform default".
    #[serde(default)]
    capture_backend: Option<capture::BackendType>,
    /// Path to the Genshin Impact game executable, so it can be launched from
    /// the UI (which also guarantees capture is running first).
    #[serde(default)]
    genshin_exe: Option<PathBuf>,
}

impl Default for SavedAppState {
    fn default() -> Self {
        Self {
            export_settings: ExportSettings {
                include_characters: true,
                include_artifacts: true,
                include_weapons: true,
                include_materials: true,
                fake_initialize_4th_line: false,
                min_character_level: 1,
                min_character_ascension: 0,
                min_character_constellation: 0,
                min_artifact_level: 0,
                min_artifact_rarity: 5,
                min_weapon_level: 1,
                min_weapon_refinement: 0,
                min_weapon_ascension: 0,
                min_weapon_rarity: 3,
            },
            auto_start_capture: false,
            log_raw_packets: false,
            tracing_level: Default::default(),
            capture_backend: None,
            genshin_exe: None,
        }
    }
}

#[derive(Clone, Debug)]
enum OptimizerExportTarget {
    None,
    Clipboard,
    File,
}

pub struct IrminsulApp {
    ui_message_tx: mpsc::UnboundedSender<Message>,
    state_rx: watch::Receiver<AppState>,
    wish_url_rx: watch::Receiver<Option<String>>,
    log_packets_tx: watch::Sender<bool>,
    tracing_reload_handle: ReloadHandle,

    toasts: Toasts,

    power_tools_open: bool,
    bug_report_open: bool,

    capture_settings_open: bool,

    optimizer_settings_open: bool,
    optimizer_export_rx: Option<oneshot::Receiver<Result<String>>>,
    optimizer_save_dialog: Option<FileDialog>,
    optimizer_save_path: Option<PathBuf>,
    optimizer_export_target: OptimizerExportTarget,

    achievement_export_rx: Option<oneshot::Receiver<Result<String>>>,
    achievement_save_dialog: Option<FileDialog>,
    achievement_save_path: Option<PathBuf>,
    achievement_export_target: OptimizerExportTarget,

    restarting: bool,

    capture_health: CaptureHealth,

    /// Directory picker for locating the Genshin Impact install.
    genshin_dir_dialog: Option<FileDialog>,

    saved_state: SavedAppState,
}

trait ToastError<T> {
    fn toast_error(self, app: &mut IrminsulApp) -> Option<T>;
}

impl<T, E: Display> ToastError<T> for std::result::Result<T, E> {
    fn toast_error(self, app: &mut IrminsulApp) -> Option<T> {
        match self {
            Ok(val) => Some(val),
            Err(e) => {
                tracing::error!("{e}");
                app.toasts.error(e.to_string());
                None
            }
        }
    }
}

fn start_async_runtime(
    egui_ctx: Context,
    log_packets_rx: watch::Receiver<bool>,
    capture_backend: capture::BackendType,
) -> (
    mpsc::UnboundedSender<Message>,
    watch::Receiver<AppState>,
    watch::Receiver<Option<String>>,
) {
    tracing::info!("starting tokio async");
    let (ui_message_tx, mut ui_message_rx) = mpsc::unbounded_channel::<Message>();

    let (state_tx, state_rx) = watch::channel(AppState::new());
    let (wish_url_tx, wish_url_rx) = watch::channel(None);
    let mut updater_state_rx = state_rx.clone();
    let updater_ctx = egui_ctx.clone();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            // Before starting the monitor, check for updates if not in debug mode
            tracing::info!("Checking for update");
            if let Err(e) = check_for_app_update(&state_tx, &mut ui_message_rx).await {
                tracing::error!("error checking for update: {e}");
            }

            // Check for wish URL
            tokio::spawn(async move {
                let Ok(mut wish) = wish::Wish::new(wish_url_tx).await else {
                    tracing::error!("Failed to create new wish monitor");
                    return;
                };

                if let Err(e) = wish.monitor().await {
                    tracing::error!("Error monitoring for wishes: {e}");
                }
            });

            // Notify egui of state changes.
            tokio::spawn(async move {
                loop {
                    let _ = updater_state_rx.changed().await;
                    updater_ctx.request_repaint();
                }
            });
            tracing::info!("Starting monitor");
            let monitor = match Monitor::new(
                state_tx,
                ui_message_rx,
                log_packets_rx,
                capture_backend,
            )
            .await
            {
                Ok(monitor) => monitor,
                Err(e) => {
                    tracing::error!("error loading monitor task: {e}");
                    return;
                }
            };
            monitor.run().await;
        });
    });
    tracing::info!("started tokio");
    (ui_message_tx, state_rx, wish_url_rx)
}

impl IrminsulApp {
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        mut tracing_reload_handle: ReloadHandle,
        capture_backend: capture::BackendType,
        capture_health: CaptureHealth,
    ) -> Self {
        egui_extras::install_image_loaders(&cc.egui_ctx);
        egui_material_icons::initialize(&cc.egui_ctx);

        let mut saved_state: SavedAppState = if let Some(storage) = cc.storage {
            eframe::get_value(storage, eframe::APP_KEY).unwrap_or_default()
        } else {
            Default::default()
        };

        // A persisted backend choice wins over the command-line default; a
        // command-line value with nothing saved yet becomes the saved default.
        let capture_backend = saved_state.capture_backend.unwrap_or(capture_backend);
        saved_state.capture_backend = Some(capture_backend);

        tracing_reload_handle.set_filter(saved_state.tracing_level.get_filter());
        let (log_packets_tx, log_packets_rx) = watch::channel(saved_state.log_raw_packets);
        let (ui_message_tx, state_rx, wish_url_rx) =
            start_async_runtime(cc.egui_ctx.clone(), log_packets_rx, capture_backend);

        if saved_state.auto_start_capture {
            capture_health.reset();
            if let Err(e) = ui_message_tx.send(Message::StartCapture) {
                tracing::error!("Failed to send auto start message: {e}");
            }
        }

        let toasts = Toasts::default().with_anchor(egui_notify::Anchor::BottomLeft);

        Self {
            saved_state,
            ui_message_tx,
            log_packets_tx,
            tracing_reload_handle,
            toasts,
            power_tools_open: false,
            bug_report_open: false,
            capture_settings_open: false,
            optimizer_settings_open: false,
            optimizer_export_rx: None,
            optimizer_save_dialog: None,
            optimizer_save_path: None,
            optimizer_export_target: OptimizerExportTarget::None,
            achievement_export_rx: None,
            achievement_save_dialog: None,
            achievement_save_path: None,
            achievement_export_target: OptimizerExportTarget::None,
            restarting: false,
            capture_health,
            genshin_dir_dialog: None,
            state_rx,
            wish_url_rx,
        }
    }

    /// Send a start-capture request, resetting the capture-health counters so
    /// the "dropping packets" warning reflects only the new session.
    fn start_capture(&self) {
        self.capture_health.reset();
        let _ = self.ui_message_tx.send(Message::StartCapture);
    }

    /// Locate `GenshinImpact.exe` given a path the user picked, which may be the
    /// executable itself or a directory containing it.
    fn resolve_genshin_exe(picked: &std::path::Path) -> Option<PathBuf> {
        if picked.is_file() {
            return Some(picked.to_path_buf());
        }
        for name in ["GenshinImpact.exe", "YuanShen.exe"] {
            let candidate = picked.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
        None
    }

    /// Start capture (if not already running) and launch the game.
    fn launch_genshin(&mut self) {
        let Some(exe) = self.saved_state.genshin_exe.clone() else {
            self.toasts.error("Set the Genshin Impact folder first");
            return;
        };
        if !exe.is_file() {
            self.toasts.error(format!("Not found: {}", exe.display()));
            return;
        }

        if !self.state_rx.borrow().capturing {
            self.start_capture();
        }

        let dir = exe.parent().map(PathBuf::from);
        let mut command = std::process::Command::new(&exe);
        if let Some(dir) = dir {
            command.current_dir(dir);
        }
        match command.spawn() {
            Ok(_) => {
                self.toasts
                    .info("Launching Genshin Impact — log in to capture your data");
            }
            Err(e) => {
                self.toasts.error(format!("Couldn't launch Genshin: {e}"));
            }
        }
    }
}

impl eframe::App for IrminsulApp {
    /// Called by the framework to save state before shutdown.
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        eframe::set_value(storage, eframe::APP_KEY, &self.saved_state);
    }

    /// Called each time the UI needs repainting, which may be many times per second.
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.style_mut(|style| {
            style.interaction.selectable_labels = false;
            style.interaction.tooltip_delay = 0.25;
        });

        self.toasts.show(ctx);
        if let Some(optimizer_save_dialog) = &mut self.optimizer_save_dialog {
            optimizer_save_dialog.update(ctx);
        }
        if let Some(genshin_dir_dialog) = &mut self.genshin_dir_dialog {
            genshin_dir_dialog.update(ctx);
        }
        if let Some(achievement_save_dialog) = &mut self.achievement_save_dialog {
            achievement_save_dialog.update(ctx);
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.with_layout(egui::Layout::top_down(egui::Align::LEFT), |ui| {
                egui::Image::new(egui::include_image!("../assets/background.webp"))
                    .paint_at(ui, ui.ctx().screen_rect());
            });

            ui.vertical(|ui| {
                self.title_bar(ui);
                ui.add_space(25.);

                // Handle power tools here instead of main UI to allow it to be opened
                // in other app states.
                let power_tools_shortcut = KeyboardShortcut {
                    modifiers: Modifiers {
                        command: true,
                        shift: true,
                        ..Default::default()
                    },
                    logical_key: Key::P,
                };
                ui.ctx().input_mut(|i| {
                    if i.consume_shortcut(&power_tools_shortcut) {
                        self.power_tools_open = true;
                    }
                });

                if self.power_tools_open {
                    let modal = Modal::new(Id::new("Power Tools")).show(ui.ctx(), |ui| {
                        self.power_tools_modal(ui);
                    });
                    if modal.should_close() {
                        self.power_tools_open = false;
                    }
                }

                if self.bug_report_open {
                    let modal = Modal::new(Id::new("Bug Report")).show(ui.ctx(), |ui| {
                        self.bug_report_modal(ui);
                    });
                    if modal.should_close() {
                        self.bug_report_open = false;
                    }
                }

                ui.horizontal(|ui| {
                    ui.add_space(525.);
                    let state = self.state_rx.borrow_and_update().clone();
                    ui.vertical(|ui| match state.state {
                        State::Starting => (),
                        State::CheckingForUpdate => self.checking_for_update_ui(ui),
                        State::WaitingForUpdateConfirmation(status) => {
                            self.waiting_for_update_confirmation_ui(ui, status)
                        }
                        State::Updating => self.updating_ui(ui),
                        State::Updated { needs_caps } => self.updated_ui(ui, needs_caps),
                        State::CheckingForData => self.checking_for_data_ui(ui),
                        State::WaitingForDownloadConfirmation(confirmation_type) => {
                            self.waiting_for_download_confirmation_ui(ui, confirmation_type)
                        }
                        State::Downloading => self.load_data_ui(ui),
                        State::Main => self.main_ui(ui, &state),
                    });
                });
            });

            ui.with_layout(egui::Layout::bottom_up(egui::Align::RIGHT), |ui| {
                ui.horizontal(|ui| {
                    let discord_icon = egui::include_image!("../assets/discord.svg");
                    if ui
                        .add(
                            Button::image(discord_icon)
                                .frame(false)
                                .image_tint_follows_text_color(true),
                        )
                        .clicked()
                    {
                        ui.ctx()
                            .open_url(OpenUrl::new_tab("https://discord.gg/aQqdZPHEpP"));
                    }

                    use egui::special_emojis::GITHUB;
                    if ui
                        .add(Button::new(RichText::new(GITHUB).size(16.)).frame(false))
                        .clicked()
                    {
                        ui.ctx()
                            .open_url(OpenUrl::new_tab("https://github.com/konkers/irminsul"));
                    }

                    let button = ui.add(
                        Button::new(
                            RichText::new(egui_material_icons::icons::ICON_BUG_REPORT).size(16.),
                        )
                        .frame(false),
                    );
                    if button.clicked() {
                        self.bug_report_open = true;
                    }
                    ui.label(env!("CARGO_PKG_VERSION").to_string());
                    egui::warn_if_debug_build(ui);
                });
            });
        });
    }
}

impl IrminsulApp {
    fn title_bar(&self, ui: &mut egui::Ui) {
        let (_, button_width) = egui::Sides::new().show(
            ui,
            |_ui| {},
            |ui| {
                let button = ui.add(
                    Button::new(RichText::new(egui_material_icons::icons::ICON_CLOSE).size(24.))
                        .frame(false),
                );
                if button.clicked() {
                    ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
                }
                button.rect.width()
            },
        );

        let app_rect = ui.max_rect();

        let title_bar_height = 32.0;
        let title_bar_rect = {
            let mut rect = app_rect;
            rect.max.y = rect.min.y + title_bar_height;
            rect.max.x -= button_width;
            rect
        };

        let response = ui.interact(
            title_bar_rect,
            Id::new("title_bar"),
            Sense::click_and_drag(),
        );

        if response.drag_started_by(PointerButton::Primary) {
            ui.ctx().send_viewport_cmd(ViewportCommand::StartDrag);
        }
    }

    fn checking_for_update_ui(&self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Checking for Irminsul updates".to_string());
        });
    }

    fn waiting_for_update_confirmation_ui(&self, ui: &mut egui::Ui, version: String) {
        ui.label(format!(
            "Update {} available.  Download and install?",
            version
        ));

        ui.horizontal(|ui| {
            if ui.add(egui::Button::new("Yes")).clicked() {
                if let Err(e) = self.ui_message_tx.send(Message::UpdateAcknowledged) {
                    tracing::error!("Unable to send UI message: {e}");
                }
            }
            if ui.add(egui::Button::new("No")).clicked() {
                if let Err(e) = self.ui_message_tx.send(Message::UpdateCanceled) {
                    tracing::error!("Unable to send UI message: {e}");
                }
            }
        });
    }

    fn updating_ui(&self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Downloading and updating...".to_string());
            ui.spinner();
        });
    }

    /// Relaunch Irminsul and close this instance.
    ///
    /// Uses the executable's own path rather than argv[0], which may be a bare
    /// name that only resolves through PATH, or be relative to a directory we
    /// are no longer in.
    fn restart(&mut self, ui: &mut egui::Ui) {
        match std::env::current_exe() {
            Ok(exe_path) => {
                let _ = std::process::Command::new(exe_path).spawn();
            }
            Err(e) => tracing::error!("unable to find Irminsul to restart it: {e}"),
        }
        ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
        self.restarting = true;
    }

    fn updated_ui(&mut self, ui: &mut egui::Ui, needs_caps: bool) {
        if needs_caps {
            return self.update_reset_permissions_ui(ui);
        }

        ui.horizontal(|ui| {
            ui.label("Updated. Restarting...".to_string());
        });
        if !self.restarting {
            self.restart(ui);
        }
    }

    /// Shown when the update dropped the packet capture permissions the user
    /// had granted.  Restarting without re-granting them just lands in the
    /// missing permissions dialog, so ask here instead of restarting straight
    /// away.
    fn update_reset_permissions_ui(&mut self, ui: &mut egui::Ui) {
        let exe_path = admin::current_exe_path();
        let command = admin::setcap_command(&exe_path);

        ui.label("Updated.  The update reset Irminsul's packet capture");
        ui.label("permissions.  Re-grant them with:");
        ui.add_space(5.0);
        ui.label(&command);
        ui.add_space(5.0);

        ui.horizontal(|ui| {
            if ui.button("Copy").clicked() {
                ui.ctx().copy_text(command.clone());
            }
            if ui.button("Restart").clicked() && !self.restarting {
                self.restart(ui);
            }
        });
    }

    fn checking_for_data_ui(&self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Checking for game data updates".to_string());
        });
    }

    fn waiting_for_download_confirmation_ui(
        &self,
        ui: &mut egui::Ui,
        confirmation_type: ConfirmationType,
    ) {
        let label = match confirmation_type {
            ConfirmationType::Initial => "Irminsul needs to download initial data",
            ConfirmationType::Update => "New data available",
        };
        ui.label(label.to_string());
        if ui.add(egui::Button::new("Download")).clicked() {
            if let Err(e) = self.ui_message_tx.send(Message::DownloadAcknowledged) {
                tracing::error!("Unable to send UI message{e}");
            }
        }
    }

    fn load_data_ui(&self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Downloading Data".to_string());
            ui.spinner();
        });
    }

    fn main_ui(&mut self, ui: &mut egui::Ui, app_state: &AppState) {
        if self.capture_settings_open {
            let modal = Modal::new(Id::new("Capture Settings")).show(ui.ctx(), |ui| {
                self.capture_settings_modal(ui);
            });
            if modal.should_close() {
                self.capture_settings_open = false;
            }
        }

        if self.optimizer_settings_open {
            let modal = Modal::new(Id::new("Optimizer Settings")).show(ui.ctx(), |ui| {
                self.optimizer_settings_modal(ui);
            });
            if modal.should_close() {
                self.optimizer_settings_open = false;
            }
        }
        self.capture_ui(ui, app_state);
        ui.separator();
        self.genshin_optimizer_ui(ui, app_state);
        ui.separator();
        self.wish_ui(ui);
        ui.separator();
        self.achievement_ui(ui, app_state);
    }

    fn capture_ui(&mut self, ui: &mut egui::Ui, app_state: &AppState) {
        ui.vertical(|ui| {
            egui::Sides::new().show(
                ui,
                |ui| {
                    Self::section_header(ui, "Packet Capture");
                },
                |ui| {
                    if ui
                        .button(egui_material_icons::icons::ICON_SETTINGS)
                        .clicked()
                    {
                        self.capture_settings_open = true;
                    }

                    if app_state.capturing {
                        if ui.button(egui_material_icons::icons::ICON_PAUSE).clicked() {
                            let _ = self.ui_message_tx.send(Message::StopCapture);
                        }
                    } else if ui
                        .button(egui_material_icons::icons::ICON_PLAY_ARROW)
                        .clicked()
                    {
                        self.start_capture();
                    }
                },
            );
        });
        let counts = app_state.counts;
        egui::Grid::new("capture_stats")
            .striped(false)
            .num_columns(3)
            .min_col_width(0.)
            .show(ui, |ui| {
                Self::data_state(
                    ui,
                    "Characters",
                    app_state.updated.characters_updated,
                    Some(counts.characters),
                );
                Self::data_state(
                    ui,
                    "Artifacts",
                    app_state.updated.items_updated,
                    Some(counts.artifacts),
                );
                Self::data_state(
                    ui,
                    "Weapons",
                    app_state.updated.items_updated,
                    Some(counts.weapons),
                );
                Self::data_state(
                    ui,
                    "Achievements",
                    app_state.updated.achievements_updated,
                    Some(counts.achievements),
                );
            });

        // Surface a hard capture failure (e.g. Npcap missing) prominently.
        if let Some(error) = &app_state.capture_error {
            ui.add_space(4.0);
            ui.colored_label(
                Color32::from_rgb(0xE0, 0x4A, 0x4A),
                format!("{}  {error}", egui_material_icons::icons::ICON_ERROR),
            );
        }

        // Warn when the backend is running but not decoding anything: the game
        // was likely already logged in when capture started, so the login data
        // dump was missed.
        if app_state.capturing
            && app_state.capture_error.is_none()
            && counts.characters == 0
            && counts.artifacts == 0
            && counts.achievements == 0
        {
            ui.add_space(4.0);
            ui.colored_label(
                Color32::from_rgb(0xE5, 0x7C, 0x00),
                format!(
                    "{}  Capturing… log in to Genshin now. If you were already \
                     in-game, fully quit and relaunch it.",
                    egui_material_icons::icons::ICON_INFO,
                ),
            );
        }

        // Warn when the capture backend is losing packets: the passive sniffer
        // can't recover dropped KCP segments, so the login data won't decode.
        if self.capture_health.is_lossy() {
            ui.add_space(4.0);
            ui.colored_label(
                Color32::from_rgb(0xE5, 0x7C, 0x00),
                format!(
                    "{}  Capture is dropping packets ({} events). Data may be \
                     incomplete — switch the capture backend to pcap in \
                     settings, then recapture.",
                    egui_material_icons::icons::ICON_WARNING,
                    self.capture_health.drop_events(),
                ),
            );
        }

        // One-click: make sure capture is running, then launch the game.
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            if ui
                .button(format!(
                    "{}  Launch Genshin",
                    egui_material_icons::icons::ICON_ROCKET_LAUNCH
                ))
                .on_hover_text("Starts capture (if needed), then launches the game")
                .clicked()
            {
                self.launch_genshin();
            }
            if ui
                .button(egui_material_icons::icons::ICON_FOLDER_OPEN)
                .on_hover_text(match &self.saved_state.genshin_exe {
                    Some(p) => p.display().to_string(),
                    None => "Choose the Genshin Impact game folder".to_string(),
                })
                .clicked()
            {
                let mut dialog = FileDialog::new();
                if let Some(exe) = &self.saved_state.genshin_exe
                    && let Some(dir) = exe.parent()
                {
                    dialog = dialog.initial_directory(dir.to_path_buf());
                }
                dialog.pick_directory();
                self.genshin_dir_dialog = Some(dialog);
            }
        });

        if let Some(dialog) = &mut self.genshin_dir_dialog
            && let Some(picked) = dialog.take_picked()
        {
            match Self::resolve_genshin_exe(&picked) {
                Some(exe) => {
                    self.toasts.info(format!("Genshin: {}", exe.display()));
                    self.saved_state.genshin_exe = Some(exe);
                }
                None => {
                    self.toasts
                        .error("No GenshinImpact.exe / YuanShen.exe in that folder");
                }
            }
            self.genshin_dir_dialog = None;
        }
    }

    fn data_state(
        ui: &mut egui::Ui,
        source: &str,
        last_updated: Option<Instant>,
        count: Option<u32>,
    ) {
        let updated_icon = match last_updated {
            Some(_) => RichText::new(egui_material_icons::icons::ICON_CHECK_CIRCLE)
                .color(Color32::from_hex("#00ab3f").unwrap()),
            None => RichText::new(egui_material_icons::icons::ICON_CHECK_INDETERMINATE_SMALL),
        };
        ui.label(updated_icon);
        ui.label(source);
        match count {
            Some(count) if last_updated.is_some() => {
                ui.label(RichText::new(count.to_string()).weak());
            }
            _ => {
                ui.label("");
            }
        }
        ui.end_row();
    }

    fn genshin_optimizer_ui(&mut self, ui: &mut egui::Ui, app_state: &AppState) {
        self.optimizer_handle_export(ui).toast_error(self);

        ui.vertical(|ui| {
            egui::Sides::new().show(
                ui,
                |ui| {
                    Self::section_header(ui, "Genshin Optimizer");
                },
                |ui| {
                    if ui
                        .button(egui_material_icons::icons::ICON_SETTINGS)
                        .clicked()
                    {
                        self.optimizer_settings_open = true;
                    }

                    ui.add_enabled_ui(
                        app_state.updated.characters_updated.is_some()
                            && app_state.updated.items_updated.is_some()
                            && self.optimizer_export_rx.is_none(),
                        |ui| {
                            if ui
                                .button(egui_material_icons::icons::ICON_DOWNLOAD)
                                .clicked()
                            {
                                let now = Local::now();
                                let mut optimizer_save_dialog = FileDialog::new()
                                    .add_file_filter_extensions("JSON files", vec!["json"])
                                    .default_file_name(&format!(
                                        "genshin_export_{}.json",
                                        now.format("%Y-%m-%d_%H-%M")
                                    ));
                                optimizer_save_dialog.save_file();
                                self.optimizer_save_dialog = Some(optimizer_save_dialog);
                            }

                            if let Some(optimizer_save_dialog) = &mut self.optimizer_save_dialog
                                && let Some(path) = optimizer_save_dialog.take_picked()
                            {
                                self.optimizer_save_path = Some(path);
                                self.genshin_optimizer_request_export(OptimizerExportTarget::File);
                            }

                            if ui
                                .button(egui_material_icons::icons::ICON_CONTENT_PASTE_GO)
                                .clicked()
                            {
                                self.genshin_optimizer_request_export(
                                    OptimizerExportTarget::Clipboard,
                                );
                            }
                        },
                    );
                },
            );
        });
    }

    fn genshin_optimizer_request_export(&mut self, target: OptimizerExportTarget) {
        let (tx, rx) = oneshot::channel();
        let _ = self.ui_message_tx.send(Message::ExportGenshinOptimizer(
            self.saved_state.export_settings.clone(),
            tx,
        ));
        self.optimizer_export_target = target;
        self.optimizer_export_rx = Some(rx);
    }

    fn wish_ui(&mut self, ui: &mut egui::Ui) {
        self.optimizer_handle_export(ui).toast_error(self);

        let wish_url = self.wish_url_rx.borrow_and_update().clone();
        ui.vertical(|ui| {
            egui::Sides::new().show(
                ui,
                |ui| {
                    Self::section_header(ui, "Wish History");
                    ui.label(egui_material_icons::icons::ICON_HELP)
                        .on_hover_text("Click the Copy icon to copy the wish URL to the clipboard.  Paste this into paimon.moe using the Manual auto-import method.");
                },
                |ui| {
                    ui.add_enabled_ui(wish_url.is_some(), |ui| {
                        if ui
                            .button(egui_material_icons::icons::ICON_CONTENT_PASTE_GO)
                            .clicked()
                        {
                            if let Some(url) = wish_url {
                                ui.ctx().copy_text(url);
                            }
                        }
                    });
                },
            );
        });
    }

    fn power_tools_modal(&mut self, ui: &mut egui::Ui) {
        ui.set_width(300.0);
        ui.heading("Power Tools");
        ui.separator();
        if ui
            .checkbox(&mut self.saved_state.log_raw_packets, "Log raw packets")
            .changed()
        {
            let _ = self.log_packets_tx.send(self.saved_state.log_raw_packets);
        };
        let prev_level = self.saved_state.tracing_level;
        egui::ComboBox::from_label("Logging Level")
            .selected_text(format!("{}", self.saved_state.tracing_level))
            .show_ui(ui, |ui| {
                ui.selectable_value(
                    &mut self.saved_state.tracing_level,
                    TracingLevel::Default,
                    "Default",
                );
                ui.selectable_value(
                    &mut self.saved_state.tracing_level,
                    TracingLevel::VerboseInfo,
                    "Verbose Info",
                );
                ui.selectable_value(
                    &mut self.saved_state.tracing_level,
                    TracingLevel::VerboseDebug,
                    "Verbose Debug",
                );
                ui.selectable_value(
                    &mut self.saved_state.tracing_level,
                    TracingLevel::VerboseTrace,
                    "Verbose Trace",
                );
            });
        if prev_level != self.saved_state.tracing_level {
            self.tracing_reload_handle
                .set_filter(self.saved_state.tracing_level.get_filter());
        }
        ui.end_row();
        ui.separator();
        egui::Sides::new().show(
            ui,
            |_ui| {},
            |ui| {
                if ui.button("Ok").clicked() {
                    ui.close()
                }
            },
        );
    }

    fn bug_report_modal(&mut self, ui: &mut egui::Ui) {
        ui.set_width(300.0);
        ui.heading("Bug Report");
        ui.separator();
        ui.label("When filing a bug, please include the latest log file:");
        if ui.button("Open log directory").clicked() {
            thread::spawn(|| {
                let _ = open_log_dir();
            });
        }
        ui.separator();
        egui::Sides::new().show(
            ui,
            |_ui| {},
            |ui| {
                if ui.button("New GitHub Issue").clicked() {
                    ui.ctx().open_url(OpenUrl::new_tab(
                        "https://github.com/konkers/irminsul/issues/new",
                    ));
                    ui.close()
                }
                if ui.button("Cancel").clicked() {
                    ui.close()
                }
            },
        );
    }

    fn capture_settings_modal(&mut self, ui: &mut egui::Ui) {
        ui.set_width(320.0);
        ui.heading("Capture Settings");
        ui.separator();
        ui.checkbox(
            &mut self.saved_state.auto_start_capture,
            "Start capture on Irminsul launch",
        );

        ui.separator();
        let current = self
            .saved_state
            .capture_backend
            .unwrap_or(capture::DEFAULT_CAPTURE_BACKEND_TYPE);
        let mut selected = current;
        egui::ComboBox::from_label("Capture backend")
            .selected_text(selected.to_string())
            .show_ui(ui, |ui| {
                ui.selectable_value(
                    &mut selected,
                    capture::BackendType::Pktmon,
                    capture::BackendType::Pktmon.to_string(),
                );
                ui.selectable_value(
                    &mut selected,
                    capture::BackendType::Pcap,
                    capture::BackendType::Pcap.to_string(),
                );
            });
        if selected != current {
            self.saved_state.capture_backend = Some(selected);
            let _ = self
                .ui_message_tx
                .send(Message::SetCaptureBackend(selected));
            // Re-arm the capture with the new backend if one is running.
            if self.state_rx.borrow().capturing {
                let _ = self.ui_message_tx.send(Message::StopCapture);
                self.start_capture();
                self.toasts.info("Capture restarted with the new backend");
            }
        }
        ui.label(
            egui::RichText::new(
                "pcap needs Npcap installed but survives Genshin's login burst; \
                 pktmon drops packets on some systems.",
            )
            .small()
            .weak(),
        );

        ui.separator();
        ui.label("Genshin Impact folder");
        ui.horizontal(|ui| {
            let label = match &self.saved_state.genshin_exe {
                Some(p) => p.display().to_string(),
                None => "(not set)".to_string(),
            };
            if ui
                .button(egui_material_icons::icons::ICON_FOLDER_OPEN)
                .clicked()
            {
                let mut dialog = FileDialog::new();
                if let Some(exe) = &self.saved_state.genshin_exe
                    && let Some(dir) = exe.parent()
                {
                    dialog = dialog.initial_directory(dir.to_path_buf());
                }
                dialog.pick_directory();
                self.genshin_dir_dialog = Some(dialog);
            }
            ui.add(egui::Label::new(egui::RichText::new(label).small()).truncate());
        });

        ui.separator();
        egui::Sides::new().show(
            ui,
            |_ui| {},
            |ui| {
                if ui.button("Ok").clicked() {
                    ui.close()
                }
            },
        );
    }

    fn optimizer_settings_modal(&mut self, ui: &mut egui::Ui) {
        ui.set_width(300.0);
        ui.heading("Genshin Optimizer Settings");
        ui.separator();
        ui.checkbox(
            &mut self.saved_state.export_settings.include_characters,
            "Characters",
        );
        ui.horizontal(|ui| {
            ui.add_space(20.);
            egui::Grid::new("char_options")
                .striped(true)
                .show(ui, |ui| {
                    ui.label("Min level".to_string());
                    ui.add(
                        DragValue::new(&mut self.saved_state.export_settings.min_character_level)
                            .range(1..=90),
                    );
                    ui.end_row();
                    ui.label("Min ascension".to_string());
                    ui.add(
                        DragValue::new(
                            &mut self.saved_state.export_settings.min_character_ascension,
                        )
                        .range(0..=6),
                    );
                    ui.end_row();
                    ui.label("Min constellation".to_string());
                    ui.add(
                        DragValue::new(
                            &mut self.saved_state.export_settings.min_character_constellation,
                        )
                        .range(0..=6),
                    );
                    ui.end_row();
                });
        });
        ui.checkbox(
            &mut self.saved_state.export_settings.include_artifacts,
            "Artifacts",
        );
        ui.horizontal(|ui| {
            ui.add_space(20.);
            egui::Grid::new("artifact_options")
                .striped(true)
                .show(ui, |ui| {
                    ui.label("Min level".to_string());
                    ui.add(
                        DragValue::new(&mut self.saved_state.export_settings.min_artifact_level)
                            .range(0..=20),
                    );
                    ui.end_row();
                    ui.label("Min rarity".to_string());
                    ui.add(
                        DragValue::new(&mut self.saved_state.export_settings.min_artifact_rarity)
                            .range(0..=6),
                    );
                    ui.end_row();
                });
        });
        ui.checkbox(
            &mut self.saved_state.export_settings.include_weapons,
            "Weapons",
        );
        ui.horizontal(|ui| {
            ui.add_space(20.);
            egui::Grid::new("weapon_options")
                .striped(true)
                .show(ui, |ui| {
                    ui.label("Min level".to_string());
                    ui.add(
                        DragValue::new(&mut self.saved_state.export_settings.min_weapon_level)
                            .range(1..=90),
                    );
                    ui.end_row();

                    ui.label("Min refinement".to_string());
                    ui.add(
                        DragValue::new(&mut self.saved_state.export_settings.min_weapon_refinement)
                            .range(1..=5),
                    );
                    ui.end_row();

                    ui.label("Min ascension".to_string());
                    ui.add(
                        DragValue::new(&mut self.saved_state.export_settings.min_weapon_ascension)
                            .range(0..=6),
                    );
                    ui.end_row();

                    ui.label("Min rarity".to_string());
                    ui.add(
                        DragValue::new(&mut self.saved_state.export_settings.min_weapon_rarity)
                            .range(1..=5),
                    );
                    ui.end_row();
                });
        });
        ui.checkbox(
            &mut self.saved_state.export_settings.include_materials,
            "Materials",
        );
        ui.checkbox(
            &mut self.saved_state.export_settings.fake_initialize_4th_line,
            "Fake level-up 5* artifacts with unactivated stats (hover for more info)"
        ).on_hover_text(
            "Genshin Optimizer still internally treats 5* 3-liners like pre-6.0, where the new stat is \"hidden\" and unknown to GO's optimizer.\nThis is a temporary workaround by activating that last stat line, but to prevent unintended effects, the artifacts are set to level 4, mimicking the player leveling it up.\nThe last line *should* be the unlockable 4th line."
        );
        ui.separator();
        egui::Sides::new().show(
            ui,
            |_ui| {},
            |ui| {
                if ui.button("Ok").clicked() {
                    ui.close()
                }
            },
        );
    }

    fn optimizer_handle_export(&mut self, ui: &mut egui::Ui) -> Result<()> {
        let Some(rx) = self.optimizer_export_rx.take() else {
            return Ok(());
        };

        let json = rx.blocking_recv()??;

        match self.optimizer_export_target {
            OptimizerExportTarget::None => {
                tracing::warn!("Unexpected json export");
            }
            OptimizerExportTarget::Clipboard => {
                self.optimizer_save_to_clipboard(ui, json)?;
            }
            OptimizerExportTarget::File => {
                self.optimizer_save_to_file(json)?;
            }
        }

        self.optimizer_export_target = OptimizerExportTarget::None;
        Ok(())
    }

    fn optimizer_save_to_clipboard(&mut self, ui: &mut egui::Ui, json: String) -> Result<()> {
        ui.ctx().copy_text(json);
        self.toasts
            .info("Genshin Optimizer data copied to clipboard");
        Ok(())
    }

    fn optimizer_save_to_file(&mut self, json: String) -> Result<()> {
        let path = self
            .optimizer_save_path
            .take()
            .ok_or_else(|| anyhow!("No save file path set"))?;

        let file = File::create(&path).with_context(|| format!("Unable to open file {path:?}"))?;
        let mut writer = BufWriter::new(file);
        writer.write_all(json.as_bytes())?;

        self.toasts.info("Genshin Optimizer data saved to file");
        Ok(())
    }

    fn achievement_ui(&mut self, ui: &mut egui::Ui, app_state: &AppState) {
        self.achievement_handle_export(ui).toast_error(self);

        ui.vertical(|ui| {
            egui::Sides::new().show(
                ui,
                |ui| {
                    Self::section_header(ui, "Achievement Export");
                    ui.label(egui_material_icons::icons::ICON_HELP)
                        .on_hover_text(
                            "Exports the UIAF format used by Snap Hutao, cocogoat and paimon.moe.",
                        );
                },
                |ui| {
                    ui.add_enabled_ui(
                        app_state.updated.achievements_updated.is_some()
                            && self.achievement_export_rx.is_none(),
                        |ui| {
                            if ui
                                .button(egui_material_icons::icons::ICON_DOWNLOAD)
                                .clicked()
                            {
                                let now = Local::now();
                                let mut dialog = FileDialog::new()
                                    .add_file_filter_extensions("JSON files", vec!["json"])
                                    .default_file_name(&format!(
                                        "achievements_uiaf_{}.json",
                                        now.format("%Y-%m-%d_%H-%M")
                                    ));
                                dialog.save_file();
                                self.achievement_save_dialog = Some(dialog);
                            }

                            if let Some(dialog) = &mut self.achievement_save_dialog
                                && let Some(path) = dialog.take_picked()
                            {
                                self.achievement_save_path = Some(path);
                                self.achievement_request_export(OptimizerExportTarget::File);
                            }

                            if ui
                                .button(egui_material_icons::icons::ICON_CONTENT_PASTE_GO)
                                .clicked()
                            {
                                self.achievement_request_export(OptimizerExportTarget::Clipboard);
                            }
                        },
                    );
                },
            );
        });
    }

    fn achievement_request_export(&mut self, target: OptimizerExportTarget) {
        let (tx, rx) = oneshot::channel();
        let _ = self.ui_message_tx.send(Message::ExportAchievements(tx));
        self.achievement_export_target = target;
        self.achievement_export_rx = Some(rx);
    }

    fn achievement_handle_export(&mut self, ui: &mut egui::Ui) -> Result<()> {
        let Some(rx) = self.achievement_export_rx.take() else {
            return Ok(());
        };

        let json = rx.blocking_recv()??;

        match self.achievement_export_target {
            OptimizerExportTarget::None => tracing::warn!("Unexpected achievement export"),
            OptimizerExportTarget::Clipboard => {
                ui.ctx().copy_text(json);
                self.toasts.info("Achievements copied to clipboard (UIAF)");
            }
            OptimizerExportTarget::File => {
                let path = self
                    .achievement_save_path
                    .take()
                    .ok_or_else(|| anyhow!("No save file path set"))?;
                let file =
                    File::create(&path).with_context(|| format!("Unable to open file {path:?}"))?;
                BufWriter::new(file).write_all(json.as_bytes())?;
                self.toasts.info("Achievements saved to file (UIAF)");
            }
        }

        self.achievement_export_target = OptimizerExportTarget::None;
        Ok(())
    }

    fn section_header(ui: &mut egui::Ui, name: &str) {
        ui.label(RichText::new(name).size(18.));
    }
}
