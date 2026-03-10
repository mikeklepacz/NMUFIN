from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
import subprocess
from threading import Thread
from urllib.parse import quote
import uuid

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import ROOT_DIR, get_app_password, sign_auth_token
from .db import init_db
from .services.bootstrap import import_sample_history, list_import_batches, sample_transaction_dates
from .services.fx import fetch_nbp_rates, get_rate_coverage, list_rates, upsert_manual_rates
from .services.funding_case import get_bank_options as get_funding_case_bank_options, get_funding_case_data
from .services.imports import ImportPreview, build_preview, commit_preview, reprice_pending_transactions
from .services.payables import (
    backfill_payable_categories,
    create_payable,
    delete_payable,
    get_fixed_cost_trends,
    get_payables_summary,
    get_vendor_options,
    list_payables,
    mark_payable_paid,
)
from .services.reports import get_bank_options, get_dashboard_data, get_transactions
from .services.review_reports import (
    get_categories,
    get_category_options,
    get_review_queue,
    get_review_totals,
    get_review_vendor_groups,
    get_transaction_status_counts,
)
from .services.review_actions import build_review_redirect, delete_transaction, next_review_focus_id
from .services.rules import create_repeat_rule_for_transaction, reapply_rules_to_transactions
from .services.settings import clear_saved_rules, create_category, delete_category, get_category_settings, get_saved_rules, update_category
from .services.transactions import (
    get_transaction_metrics,
    get_transaction_vendor_options,
    list_transactions,
    transaction_query_string,
    update_transaction,
)
from .services.translation import rebuild_all_transaction_translations
from .normalization import clean_text, infer_vendor


def format_money(value: float | int | None, currency: str = "PLN") -> str:
    if value is None or value == "":
        return ""
    symbol = {"USD": "$", "EUR": "EUR ", "PLN": "PLN "}.get(currency, f"{currency} ")
    return f"{symbol}{float(value):,.2f}"


@dataclass(slots=True)
class ImportJob:
    job_id: str
    filename: str
    status: str
    stage: str
    total_rows: int
    processed_rows: int
    redirect_url: str | None = None
    error: str | None = None


def update_import_job(
    app: FastAPI,
    job_id: str,
    *,
    status: str | None = None,
    stage: str | None = None,
    total_rows: int | None = None,
    processed_rows: int | None = None,
    redirect_url: str | None = None,
    error: str | None = None,
) -> None:
    job = app.state.import_jobs.get(job_id)
    if job is None:
        return
    if status is not None:
        job.status = status
    if stage is not None:
        job.stage = stage
    if total_rows is not None:
        job.total_rows = total_rows
    if processed_rows is not None:
        job.processed_rows = processed_rows
    if redirect_url is not None:
        job.redirect_url = redirect_url
    if error is not None:
        job.error = error


def run_import_commit_job(app: FastAPI, job_id: str, preview_id: str) -> None:
    preview = app.state.previews.get(preview_id)
    if preview is None:
        update_import_job(app, job_id, status="failed", stage="Import failed", error="Preview session was not found.")
        return
    try:
        batch_id = commit_preview(
            preview,
            progress=lambda stage, total_rows, processed_rows: update_import_job(
                app,
                job_id,
                status="running",
                stage=stage,
                total_rows=total_rows,
                processed_rows=processed_rows,
            ),
        )
        app.state.previews.pop(preview_id, None)
        update_import_job(
            app,
            job_id,
            status="completed",
            stage="Import complete",
            total_rows=preview.row_count,
            processed_rows=preview.row_count,
            redirect_url="/review",
        )
    except Exception as exc:
        update_import_job(app, job_id, status="failed", stage="Import failed", error=str(exc))

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield
app = FastAPI(title="NMU FIN", lifespan=lifespan)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.filters["money"] = format_money
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
app.state.previews: dict[str, ImportPreview] = {}
app.state.import_jobs: dict[str, ImportJob] = {}

AUTH_COOKIE_NAME = "nmu_fin_auth"
AUTH_COOKIE_PAYLOAD = "authenticated"
AUTH_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365


def is_authenticated(request: Request) -> bool:
    token = request.cookies.get(AUTH_COOKIE_NAME, "")
    expected = sign_auth_token(AUTH_COOKIE_PAYLOAD)
    return bool(token) and token == expected


def git_status_snapshot() -> dict:
    try:
        branch = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        porcelain = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.splitlines()
    except subprocess.CalledProcessError:
        return {"available": False, "branch": "", "changes": 0}
    return {"available": True, "branch": branch, "changes": len([line for line in porcelain if line.strip()])}


@app.middleware("http")
async def require_login(request: Request, call_next):
    path = request.url.path
    if path.startswith("/static") or path in {"/login", "/favicon.ico"}:
        return await call_next(request)
    if is_authenticated(request):
        return await call_next(request)
    next_target = quote(str(request.url.path) + (f"?{request.url.query}" if request.url.query else ""), safe="/?=&")
    return RedirectResponse(url=f"/login?next={next_target}", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/", message: str | None = None) -> HTMLResponse:
    if is_authenticated(request):
        return RedirectResponse(url=next or "/", status_code=303)
    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "request": request,
            "next": next or "/",
            "message": message or "",
        },
    )


@app.post("/login")
def login_submit(password: str = Form(...), next: str = Form(default="/")) -> RedirectResponse:
    if password != get_app_password():
        return RedirectResponse(url=f"/login?message=Invalid%20password&next={quote(next, safe='/?=&')}", status_code=303)
    response = RedirectResponse(url=next or "/", status_code=303)
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=sign_auth_token(AUTH_COOKIE_PAYLOAD),
        max_age=AUTH_COOKIE_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
    )
    return response


@app.post("/logout")
def logout() -> RedirectResponse:
    response = RedirectResponse(url="/login?message=Logged%20out", status_code=303)
    response.delete_cookie(AUTH_COOKIE_NAME)
    return response
@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    currency: str = "PLN",
    include_transfers: bool = False,
    year: str = "all",
    year_start: str | None = None,
    year_end: str | None = None,
    focus_client: str | None = None,
    bank_filter: str | None = None,
) -> HTMLResponse:
    parsed_year_start = int(year_start) if year_start and year_start.strip() else None
    parsed_year_end = int(year_end) if year_end and year_end.strip() else None
    if parsed_year_start is None and parsed_year_end is None and year != "all":
        selected_year = int(year)
        parsed_year_start = selected_year
        parsed_year_end = selected_year
    data = get_dashboard_data(currency, include_transfers, parsed_year_start, parsed_year_end, focus_client, bank_filter)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "request": request,
            "dashboard": data,
            "currency": currency,
            "include_transfers": include_transfers,
            "year": year,
            "year_start": parsed_year_start,
            "year_end": parsed_year_end,
            "focus_client": focus_client or "",
            "bank_filter": bank_filter or "",
        },
    )


@app.get("/funding-case", response_class=HTMLResponse)
def funding_case_page(
    request: Request,
    currency: str = "PLN",
    bank_filter: str | None = None,
    burn_window_months: int = 6,
) -> HTMLResponse:
    data = get_funding_case_data(currency, bank_filter, burn_window_months)
    return templates.TemplateResponse(
        request,
        "funding_case.html",
        {
            "request": request,
            "funding_case": data,
            "currency": currency,
            "bank_filter": bank_filter or "",
            "burn_window_months": burn_window_months,
            "bank_options": get_funding_case_bank_options(),
        },
    )
@app.get("/imports", response_class=HTMLResponse)
def imports_page(request: Request, message: str | None = None) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "imports.html",
        {
            "request": request,
            "batches": list_import_batches(),
            "message": message,
        },
    )
@app.post("/imports/preview", response_class=HTMLResponse)
async def import_preview(request: Request, csv_file: list[UploadFile] = File(...)) -> HTMLResponse:
    upload_sources = [
        (upload.filename, await upload.read())
        for upload in csv_file
        if upload.filename
    ]
    preview = build_preview(upload_sources, translate_preview_text=True)
    app.state.previews[preview.preview_id] = preview
    return templates.TemplateResponse(
        request,
        "import_preview.html",
        {
            "request": request,
            "preview": preview,
        },
    )
@app.get("/imports/jobs/{job_id}/status")
def import_job_status(job_id: str) -> JSONResponse:
    job = app.state.import_jobs.get(job_id)
    if job is None:
        return JSONResponse({"error": "Import job was not found."}, status_code=404)
    return JSONResponse(asdict(job))
@app.get("/imports/previews/{preview_id}", response_class=HTMLResponse)
def import_preview_page(request: Request, preview_id: str) -> HTMLResponse:
    preview = app.state.previews.get(preview_id)
    if preview is None:
        return templates.TemplateResponse(
            request,
            "import_progress.html",
            {
                "request": request,
                "job": ImportJob(
                    job_id="missing-preview",
                    filename="",
                    status="failed",
                    stage="Preview unavailable",
                    total_rows=0,
                    processed_rows=0,
                    error="Preview session expired. Please upload the CSV again.",
                ),
                "headline": "Import Preview",
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        request,
        "import_preview.html",
        {
            "request": request,
            "preview": preview,
        },
    )
@app.post("/imports/commit")
def import_commit(request: Request, preview_id: str = Form(...)) -> HTMLResponse:
    preview = app.state.previews.get(preview_id)
    if preview is None:
        return templates.TemplateResponse(
            request,
            "import_progress.html",
            {
                "request": request,
                "job": ImportJob(
                    job_id="missing-preview",
                    filename="",
                    status="failed",
                    stage="Import failed",
                    total_rows=0,
                    processed_rows=0,
                    error="Preview session expired. Please upload the CSV again.",
                ),
                "headline": "Importing CSV",
            },
            status_code=404,
        )
    job = ImportJob(
        job_id=str(uuid.uuid4()),
        filename=preview.filename,
        status="running",
        stage="Starting import",
        total_rows=preview.row_count,
        processed_rows=0,
    )
    app.state.import_jobs[job.job_id] = job
    Thread(target=run_import_commit_job, args=(app, job.job_id, preview_id), daemon=True).start()
    return templates.TemplateResponse(
        request,
        "import_progress.html",
        {
            "request": request,
            "job": job,
            "headline": "Importing CSV",
        },
    )
@app.post("/imports/sample")
def import_sample_data() -> RedirectResponse:
    import_sample_history()
    return RedirectResponse(url="/transactions", status_code=303)
@app.post("/imports/translations/rebuild")
def rebuild_import_translations() -> RedirectResponse:
    updated = rebuild_all_transaction_translations()
    return RedirectResponse(
        url=f"/imports?message=Rebuilt%20translations%20for%20{updated}%20rows",
        status_code=303,
    )
@app.get("/review", response_class=HTMLResponse)
def review_queue(
    request: Request,
    focus_id: int | None = None,
    message: str | None = None,
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    vendor_filter: str | None = None,
    transaction_filter: str | None = None,
    sort_by: str = "date_new",
    limit: int = 200,
) -> HTMLResponse:
    items = get_review_queue(currency_filter, direction_filter, vendor_filter, transaction_filter, sort_by, limit)
    vendor_groups = get_review_vendor_groups(currency_filter, direction_filter, vendor_filter, transaction_filter)
    for group in vendor_groups:
        group["review_url"] = "/review?" + transaction_query_string(
            currency_filter=currency_filter,
            direction_filter=direction_filter,
            vendor_filter=group["vendor_name"],
            transaction_filter=transaction_filter,
            sort_by=sort_by,
            limit=limit,
        )
    return templates.TemplateResponse(
        request,
        "review.html",
        {
            "request": request,
            "items": items,
            "review_count": len(items),
            "review_totals": get_review_totals(currency_filter, direction_filter, vendor_filter, transaction_filter),
            "vendor_groups": vendor_groups,
            "categories": get_categories(),
            "category_options": get_category_options(),
            "focus_id": focus_id,
            "message": message,
            "currency_filter": currency_filter,
            "direction_filter": direction_filter,
            "vendor_filter": vendor_filter,
            "transaction_filter": transaction_filter,
            "sort_by": sort_by,
            "limit": limit,
        },
    )
@app.post("/review/{transaction_id}")
def review_update(
    transaction_id: int,
    vendor_canonical: str = Form(...),
    category_name: str = Form(...),
    category_bucket: str | None = Form(default=None),
    save_repeat_rule: str | None = Form(default=None),
    previous_vendor_canonical: str | None = Form(default=None),
    currency_filter: str | None = Form(default=None),
    direction_filter: str | None = Form(default=None),
    vendor_filter: str | None = Form(default=None),
    transaction_filter: str | None = Form(default=None),
    sort_by: str = Form(default="date_new"),
    limit: int = Form(default=200),
) -> RedirectResponse:
    from .db import connect

    next_focus_id = next_review_focus_id(
        transaction_id,
        currency_filter,
        direction_filter,
        vendor_filter,
        transaction_filter,
        sort_by,
    )
    resolved_vendor = vendor_canonical.strip()
    if not resolved_vendor:
        with connect() as conn:
            row = conn.execute(
                "SELECT description_raw, vendor_raw FROM transactions WHERE id = ?",
                [transaction_id],
            ).fetchone()
        if row:
            resolved_vendor = infer_vendor(clean_text(row[0]), row[1]) or ""
    update_transaction(transaction_id, resolved_vendor, category_name, category_bucket, previous_vendor_canonical)
    if save_repeat_rule == "true":
        create_repeat_rule_for_transaction(transaction_id, resolved_vendor, category_name)
        reapply_rules_to_transactions()
    redirect_url = build_review_redirect(
        next_focus_id,
        currency_filter,
        direction_filter,
        vendor_filter,
        transaction_filter,
        sort_by,
        limit,
    )
    return RedirectResponse(url=redirect_url, status_code=303)
@app.post("/review/{transaction_id}/delete")
def review_delete(
    transaction_id: int,
    currency_filter: str | None = Form(default=None),
    direction_filter: str | None = Form(default=None),
    vendor_filter: str | None = Form(default=None),
    transaction_filter: str | None = Form(default=None),
    sort_by: str = Form(default="date_new"),
    limit: int = Form(default=200),
) -> RedirectResponse:
    next_focus_id = next_review_focus_id(
        transaction_id,
        currency_filter,
        direction_filter,
        vendor_filter,
        transaction_filter,
        sort_by,
    )
    delete_transaction(transaction_id)
    redirect_url = build_review_redirect(
        next_focus_id,
        currency_filter,
        direction_filter,
        vendor_filter,
        transaction_filter,
        sort_by,
        limit,
    )
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/review-bulk-apply")
def review_bulk_apply(
    transaction_ids: list[int] | None = Form(default=None),
    vendor_canonical: str = Form(default=""),
    category_name: str = Form(...),
    category_bucket: str | None = Form(default=None),
    currency_filter: str | None = Form(default=None),
    direction_filter: str | None = Form(default=None),
    vendor_filter: str | None = Form(default=None),
    transaction_filter: str | None = Form(default=None),
    sort_by: str = Form(default="date_new"),
    limit: int = Form(default=200),
) -> RedirectResponse:
    selected_ids = transaction_ids or []
    if not selected_ids:
        redirect_url = build_review_redirect(
            None,
            currency_filter,
            direction_filter,
            vendor_filter,
            transaction_filter,
            sort_by,
            limit,
        )
        separator = "&" if "?" in redirect_url else "?"
        return RedirectResponse(
            url=f"{redirect_url}{separator}message=No%20transactions%20selected%20for%20bulk%20apply",
            status_code=303,
        )
    for transaction_id in selected_ids:
        update_transaction(transaction_id, vendor_canonical, category_name, category_bucket, None)
    redirect_url = build_review_redirect(
        None,
        currency_filter,
        direction_filter,
        vendor_filter,
        transaction_filter,
        sort_by,
        limit,
    )
    return RedirectResponse(url=redirect_url, status_code=303)
@app.get("/fx", response_class=HTMLResponse)
def fx_page(request: Request, limit: int = 500) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "fx.html",
        {
            "request": request,
            "rates": list_rates(limit),
            "coverage": get_rate_coverage(),
            "limit": limit,
        },
    )
@app.post("/fx")
def fx_save(
    rate_date: date = Form(...),
    usd_pln: float = Form(...),
    eur_pln: float = Form(...),
) -> RedirectResponse:
    upsert_manual_rates(rate_date, usd_pln, eur_pln)
    reprice_pending_transactions(rate_date)
    return RedirectResponse(url="/fx", status_code=303)
@app.post("/fx/fetch")
def fx_fetch(
    start_date: date = Form(...),
    end_date: date = Form(...),
) -> RedirectResponse:
    fetch_nbp_rates(start_date, end_date)
    return RedirectResponse(url="/fx", status_code=303)
@app.post("/fx/fetch/sample-history")
def fx_fetch_sample_history() -> RedirectResponse:
    from .services.fx import fetch_nbp_rates_for_dates
    fetch_nbp_rates_for_dates(sample_transaction_dates())
    return RedirectResponse(url="/fx", status_code=303)
@app.get("/transactions", response_class=HTMLResponse)
def transactions_page(
    request: Request,
    focus_id: int | None = None,
    include_duplicates: bool = False,
    bank_filter: str | None = None,
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    status_filter: str | None = None,
    vendor_filter: str | None = None,
    category_filter: str | None = None,
    sort_by: str = "last_modified",
    sort_dir: str = "desc",
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "transactions.html",
        {
            "request": request,
            "transactions": list_transactions(
                include_duplicates=include_duplicates,
                bank_filter=bank_filter,
                currency_filter=currency_filter,
                direction_filter=direction_filter,
                status_filter=status_filter,
                vendor_filter=vendor_filter,
                category_filter=category_filter,
                sort_by=sort_by,
                sort_dir=sort_dir,
            ),
            "include_duplicates": include_duplicates,
            "status_counts": get_transaction_status_counts(),
            "metrics": get_transaction_metrics(
                include_duplicates=include_duplicates,
                bank_filter=bank_filter,
                currency_filter=currency_filter,
                direction_filter=direction_filter,
                status_filter=status_filter,
                vendor_filter=vendor_filter,
                category_filter=category_filter,
            ),
            "category_options": get_category_options(),
            "vendor_options": get_transaction_vendor_options(),
            "bank_options": get_bank_options(),
            "bank_filter": bank_filter,
            "currency_filter": currency_filter,
            "direction_filter": direction_filter,
            "status_filter": status_filter,
            "vendor_filter": vendor_filter,
            "category_filter": category_filter,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "focus_id": focus_id,
        },
    )
@app.post("/transactions/{transaction_id}")
def transactions_update(
    transaction_id: int,
    vendor_canonical: str = Form(...),
    category_name: str = Form(...),
    category_bucket: str | None = Form(default=None),
    previous_vendor_canonical: str | None = Form(default=None),
    include_duplicates: str | None = Form(default=None),
    bank_filter: str | None = Form(default=None),
    currency_filter: str | None = Form(default=None),
    direction_filter: str | None = Form(default=None),
    status_filter: str | None = Form(default=None),
    vendor_filter: str | None = Form(default=None),
    category_filter: str | None = Form(default=None),
    sort_by: str = Form(default="last_modified"),
    sort_dir: str = Form(default="desc"),
) -> RedirectResponse:
    update_transaction(transaction_id, vendor_canonical, category_name, category_bucket, previous_vendor_canonical)
    query = transaction_query_string(
        focus_id=transaction_id,
        include_duplicates=include_duplicates == "true",
        bank_filter=bank_filter,
        currency_filter=currency_filter,
        direction_filter=direction_filter,
        status_filter=status_filter,
        vendor_filter=vendor_filter,
        category_filter=category_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return RedirectResponse(url=f"/transactions?{query}" if query else "/transactions", status_code=303)
@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, message: str | None = None) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "request": request,
            "categories": get_category_settings(),
            "saved_rules": get_saved_rules(),
            "parent_options": ["Income", "Expenses", "Exchange", ""],
            "message": message,
            "git_status": git_status_snapshot(),
        },
    )
@app.post("/settings/categories")
def settings_create_category(
    category_name: str = Form(...),
    parent_category: str = Form(default=""),
) -> RedirectResponse:
    message = create_category(category_name, parent_category or None)
    redirect_url = "/settings" if not message else f"/settings?message={message.replace(' ', '%20').replace('\"', '%22')}"
    return RedirectResponse(url=redirect_url, status_code=303)
@app.post("/settings/categories/{category_id}/update")
def settings_update_category(
    category_id: int,
    category_name: str = Form(...),
    parent_category: str = Form(default=""),
) -> RedirectResponse:
    message = update_category(category_id, category_name, parent_category or None)
    redirect_url = "/settings" if not message else f"/settings?message={message.replace(' ', '%20').replace('\"', '%22')}"
    return RedirectResponse(url=redirect_url, status_code=303)
@app.post("/settings/categories/{category_id}/delete")
def settings_delete_category(category_id: int) -> RedirectResponse:
    delete_category(category_id)
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/rules/clear")
def settings_clear_rules() -> RedirectResponse:
    clear_saved_rules()
    return RedirectResponse(url="/settings?message=Saved%20rules%20cleared", status_code=303)


@app.post("/settings/git/push")
def settings_git_push(
    commit_message: str = Form(default="Update from NMU FIN UI"),
    remote_name: str = Form(default="origin"),
) -> RedirectResponse:
    try:
        subprocess.run(["git", "add", "-A"], cwd=ROOT_DIR, check=True, capture_output=True, text=True)
        staged_clean = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=ROOT_DIR,
            check=False,
        ).returncode == 0
        if staged_clean:
            return RedirectResponse(url="/settings?message=No%20changes%20to%20commit", status_code=303)
        subprocess.run(
            ["git", "commit", "-m", commit_message.strip() or "Update from NMU FIN UI"],
            cwd=ROOT_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        branch = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=ROOT_DIR,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        subprocess.run(
            ["git", "push", remote_name.strip() or "origin", branch],
            cwd=ROOT_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        return RedirectResponse(url=f"/settings?message={quote(f'Pushed {branch} to {remote_name}')}", status_code=303)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip().splitlines()
        reason = stderr[-1] if stderr else str(exc)
        return RedirectResponse(url=f"/settings?message={quote(f'Git push failed: {reason}')}", status_code=303)


@app.get("/payables", response_class=HTMLResponse)
def payables_page(request: Request, status_filter: str = "open", message: str | None = None) -> HTMLResponse:
    backfill_payable_categories()
    return templates.TemplateResponse(
        request,
        "payables.html",
        {
            "request": request,
            "status_filter": status_filter,
            "message": message,
            "payables": list_payables(status_filter),
            "summary": get_payables_summary(),
            "categories": get_categories(),
            "vendor_options": get_vendor_options(),
            "fixed_cost_trends": get_fixed_cost_trends(),
        },
    )


@app.post("/payables/create")
def payables_create(
    vendor_canonical: str = Form(...),
    category_name: str = Form(default=""),
    currency_original: str = Form(...),
    amount_original: float = Form(...),
    due_date: date = Form(...),
    note: str = Form(default=""),
) -> RedirectResponse:
    create_payable(
        vendor_canonical=vendor_canonical,
        category_name=category_name or None,
        currency_original=currency_original,
        amount_original=amount_original,
        due_date=due_date,
        note=note or None,
    )
    return RedirectResponse(url="/payables?message=Payable%20added", status_code=303)


@app.post("/payables/{payable_id}/delete")
def payables_delete(payable_id: int, status_filter: str = Form(default="open")) -> RedirectResponse:
    delete_payable(payable_id)
    return RedirectResponse(url=f"/payables?status_filter={status_filter}", status_code=303)


@app.post("/payables/{payable_id}/mark-paid")
def payables_mark_paid(payable_id: int, status_filter: str = Form(default="open")) -> RedirectResponse:
    mark_payable_paid(payable_id)
    return RedirectResponse(url=f"/payables?status_filter={status_filter}", status_code=303)
