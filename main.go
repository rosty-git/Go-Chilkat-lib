package main

import (
	"chilkat"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net"
	_ "net/http/pprof"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Mailbox struct {
	Id           *int       // Указатель на int
	ProfileId    *int       // Указатель на int
	Email        *string    // Указатель на string
	Country      *string    // Указатель на string
	ImapUser     *string    // Указатель на string
	ImapPassword *string    // Указатель на string
	ImapServer   *string    // Указатель на string
	ImapPort     *int       // Указатель на int
	Proxy        *string    // Указатель на string
	Status       *string    // Указатель на string
	IsActivate   *bool      // Указатель на bool
	StatusTest   *string    // Указатель на string
	Uids         *string    // Указатель на string
	IsFlagged    *bool      // Указатель на bool
	Today        *int       // Указатель на int
	AllTime      *int       // Указатель на int
	Comments     *string    // Указатель на string
	CreatedAt    *time.Time // Указатель на time.Time
	UpdatedAt    *time.Time // Указатель на time.Time
	AdminId      *int       // Указатель на int
	Modified     *int       // Указатель на int
	ColorFlag    *string    // Указатель на string
	Xoauth2      *string    // Указатель на string
	InitStatus   string     // Добавлено новое поле для отслеживания статуса инициализации
}

type Letter struct {
	Id        *int    // Указатель на int
	EditedEml *string // Указатель на string
	MailId    *int    // Указатель на int
	Type      *string // Указатель на string
	AdminId   *int    // Указатель на int
	Status    *string // Указатель на string
	FolderEml *string
}

type ImapConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
}

type ProxyConfig struct {
	Type     int     // Версия SOCKS (4 или 5)
	User     *string // Имя пользователя для прокси
	Password *string // Пароль для прокси
	Host     string  // Хост прокси-сервера
	Port     int     // Порт прокси-сервера
}

type SharedData struct {
	IsTurn            bool
	MaxAttachmentSize int
	Keywords          []Keyword
	Exceptions        []Exception
}

type Keyword struct {
	ProfileID         int
	Keyword           string
	IsActivate        bool
	IsBody            bool
	IsSubject         bool
	IsAttachment      bool
	IsSent            bool
	IsFrom            bool
	IsOnlyAttachments bool
	IsRegex           bool
}

type Exception struct {
	ProfileID    int
	Exception    string
	IsActivate   bool
	IsBody       bool
	IsSubject    bool
	IsAttachment bool
	IsSent       bool
	IsFrom       bool
}

type MailboxRequest struct {
	IdEmail int    `json:"id_email"` // ID почтового ящика
	Id      int    `json:"id"`       // ID письма
	Type    string `json:"type"`     // Тип операции
}

type LetterInfo struct {
	Id   int
	Type string
}

type Command struct {
	Type     string
	Data     string
	Response chan string
}

type Result struct {
	ID      int
	Success bool
}

// type CommandChanMap map[int]chan string
type CommandChanMap map[int]chan Command

var (
	pool                 *pgxpool.Pool
	activeMailboxes      map[int]*Mailbox
	activeMailboxesMutex sync.Mutex
	sharedData           SharedData
	sharedDataMutex      sync.Mutex
	controlChan          = make(chan string, 100)
	commandChans         = make(CommandChanMap, 100)
	commandChansMutex    sync.Mutex
)

func main() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error Getting Rlimit ", err)
	}
	fmt.Println("Initial Rlimit: ", rLimit)

	customTimeFormat := "2006-01-02 15:04:05"
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: customTimeFormat}
	log.Logger = log.Output(output)
	log.Logger = log.With().Caller().Logger()

	// Unlimited threads and memory
	debug.SetMaxThreads(25000)
	//debug.SetMemoryLimit(math.MaxInt64)

	// Environment variables
	err = godotenv.Load()
	if err != nil {
		log.Error().
			Err(err).
			Msg("Failed to load .env file")
	}

	glob := chilkat.NewGlobal()
	defer glob.DisposeGlobal()
	success := glob.UnlockBundle(os.Getenv("CHILKAT_KEY"))
	if !success {
		fmt.Println(glob.LastErrorText())
		return
	}

	// Database connection
	pool = initializeDB()
	defer pool.Close()
	mailboxes, err := getMailboxes()
	if err != nil {
		log.Error().
			Err(err).
			Str("function", "getMailboxes").
			Msg("Failed to get mailboxes")
	}

	sharedData, _ = getSharedData()
	activeMailboxes = make(map[int]*Mailbox)

	doneCount := 0
	checkLength := true
	startingMailboxCount := len(mailboxes)
	startIntervalUpdate := make(chan struct{})

	batchSize, _ := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	batchTimeout, _ := strconv.Atoi(os.Getenv("BATCH_TIMEOUT"))

	go func() {
		for msg := range controlChan {
			parts := strings.SplitN(msg, " ", 2)
			if len(parts) == 2 {
				status, idStr := parts[0], parts[1]
				id, err := strconv.Atoi(idStr)
				if err != nil {
					log.Error().
						Err(err).
						Str("function", "strconv.Atoi").
						Msg("Error converting ID")
					continue
				}

				activeMailboxesMutex.Lock()
				commandChansMutex.Lock()
				if status == "failed" || status == "delete" {
					delete(activeMailboxes, id)
					if ch, ok := commandChans[id]; ok {
						close(ch)
						delete(commandChans, id)
					}
				}

				commandChansMutex.Unlock()
				activeMailboxesMutex.Unlock()

				if (status == "success" || status == "failed") && checkLength {
					doneCount++
					if doneCount == startingMailboxCount {
						log.Info().
							Msg("Start updating interval")
						startIntervalUpdate <- struct{}{}
					}
				}
			}
		}
	}()

	for i := 0; i < len(mailboxes); i += batchSize {
		end := i + batchSize
		if end > len(mailboxes) {
			end = len(mailboxes)
		}

		for j, mailbox := range mailboxes[i:end] {
			mailboxCopy := mailbox

			commandChan := make(chan Command, 100)

			commandChansMutex.Lock()
			commandChans[*mailboxCopy.Id] = commandChan
			commandChansMutex.Unlock()

			sharedDataMutex.Lock()
			initialDataCopy := sharedData
			sharedDataMutex.Unlock()

			go handleMailbox(mailboxCopy, controlChan, commandChan, initialDataCopy)

			log.Info().
				Int("current", i+j+1).
				Int("all", len(mailboxes)).
				Msg("Connecting mailboxes...")

			activeMailboxesMutex.Lock()
			activeMailboxes[*mailboxCopy.Id] = &mailboxCopy
			activeMailboxesMutex.Unlock()
		}

		if end < len(mailboxes) {
			time.Sleep(time.Duration(batchTimeout) * time.Second)
		}
	}

	<-startIntervalUpdate
	checkLength = false

	select {}
}

func initializeDB() *pgxpool.Pool {
	databaseURL := os.Getenv("DATABASE_URL")
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error parsing database config")
	}
	maxConns, _ := strconv.Atoi(os.Getenv("MAX_DB_CONNECTIONS"))
	config.MaxConns = int32(maxConns)

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Failed to create database pool")
	}
	return pool
}

func getSharedData() (SharedData, error) {
	var data SharedData

	rows, err := pool.Query(context.Background(), `SELECT name, value FROM settings`)
	if err != nil {
		return data, fmt.Errorf("Error of get shared data: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			return data, fmt.Errorf("Error of scan shared data: %w", err)
		}

		if name == "max_attachment_size" {
			data.MaxAttachmentSize = value
		} else if name == "is_turn" {
			data.IsTurn = value == 1.0
		}
	}

	rows, err = pool.Query(context.Background(), `SELECT profile_id, keyword, is_activate, is_body, is_subject, is_attachment, is_sent, is_from, is_only_attachments, is_regex FROM keywords WHERE is_activate = true`)
	if err != nil {
		return data, fmt.Errorf("Error of get keywords: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var keyword Keyword
		if err := rows.Scan(&keyword.ProfileID, &keyword.Keyword, &keyword.IsActivate, &keyword.IsBody, &keyword.IsSubject, &keyword.IsAttachment, &keyword.IsSent, &keyword.IsFrom, &keyword.IsOnlyAttachments, &keyword.IsRegex); err != nil {
			return data, fmt.Errorf("Error of scan keywords: %w", err)
		}
		data.Keywords = append(data.Keywords, keyword)
	}

	rows, err = pool.Query(context.Background(), `SELECT profile_id, exception, is_activate, is_body, is_subject, is_attachment, is_sent, is_from FROM exceptions WHERE is_activate = true`)
	if err != nil {
		return data, fmt.Errorf("Error of get exceptions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ex Exception
		if err := rows.Scan(&ex.ProfileID, &ex.Exception, &ex.IsActivate, &ex.IsBody, &ex.IsSubject, &ex.IsAttachment, &ex.IsSent, &ex.IsFrom); err != nil {
			return data, fmt.Errorf("Error of scan exceptions: %w", err)
		}
		data.Exceptions = append(data.Exceptions, ex)
	}

	return data, nil
}

func getMailboxes() ([]Mailbox, error) {
	query := `SELECT id, profile_id, email, country, imap_user, imap_password, imap_server, imap_port, proxy, status, is_activate, status_test, uids, is_flagged, today, all_time, comments, created_at, updated_at, admin_id, modified, color_flag, xoauth2 FROM mails WHERE is_activate = true  ORDER BY id ASC;`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("Error of get mails: %w", err)
	}
	defer rows.Close()

	var mailboxes []Mailbox
	for rows.Next() {
		var mailbox Mailbox
		err := rows.Scan(&mailbox.Id, &mailbox.ProfileId, &mailbox.Email, &mailbox.Country, &mailbox.ImapUser, &mailbox.ImapPassword, &mailbox.ImapServer, &mailbox.ImapPort, &mailbox.Proxy, &mailbox.Status, &mailbox.IsActivate, &mailbox.StatusTest, &mailbox.Uids, &mailbox.IsFlagged, &mailbox.Today, &mailbox.AllTime, &mailbox.Comments, &mailbox.CreatedAt, &mailbox.UpdatedAt, &mailbox.AdminId, &mailbox.Modified, &mailbox.ColorFlag, &mailbox.Xoauth2)
		if err != nil {
			return nil, fmt.Errorf("Scan mails error: %w", err)
		}
		mailboxes = append(mailboxes, mailbox)

		if _, err := pool.Exec(context.Background(), `UPDATE mails SET status = 'STARTING' WHERE id = $1`, *mailbox.Id); err != nil {
			return nil, fmt.Errorf("Error of update mails status: %w", err)
		}

	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Error interating rows: %w", err)
	}

	// Обновляем статус почт, где is_activate = false, на PAUSED.
	updateQuery := `UPDATE mails SET status = 'PAUSED' WHERE is_activate = false;`
	if _, err := pool.Exec(context.Background(), updateQuery); err != nil {
		return nil, fmt.Errorf("error updating status of inactive mails to PAUSED: %w", err)
	}

	return mailboxes, nil
}

func getLetter(id int) (Letter, error) {
	var letter Letter
	row := pool.QueryRow(context.Background(), `SELECT id, edited_eml, id_email, type, admin_id, status, folder_eml FROM letters WHERE id = $1;`, id)
	err := row.Scan(&letter.Id, &letter.EditedEml, &letter.MailId, &letter.Type, &letter.AdminId, &letter.Status, &letter.FolderEml)
	if err != nil {
		return Letter{}, fmt.Errorf("Error of get letter: %w", err)
	}

	return letter, nil
}

func handleMailbox(mailbox Mailbox, controlChan chan<- string, commandChan <-chan Command, localData SharedData) {
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Str("mailbox", *mailbox.ImapUser).
				Msgf("Recovered in handleMailbox: %v", r)
			log.Error().Msgf("Stack trace: %s", string(debug.Stack()))
		}
	}()

	xml := chilkat.NewXml()
	defer xml.DisposeXml()
	imap := chilkat.NewImap()
	defer imap.DisposeImap()

	if !connectIMAP(imap, mailbox) {
		//log.Error().
		//	Str("mailbox", *mailbox.ImapUser).
		//	Str("error", imap.LastErrorText()).
		//	Msg("Failed connectIMAP")
		controlChan <- fmt.Sprintf("failed %d", *mailbox.Id)
		return
	}

	success := imap.IdleStart()
	if !success {
		//log.Error().
		//	Str("mailbox", *mailbox.ImapUser).
		//	Str("error", imap.LastErrorText()).
		//	Msg("Failed IdleStart")
		controlChan <- fmt.Sprintf("failed %d", *mailbox.Id)
		return
	}
	controlChan <- fmt.Sprintf("success %d", *mailbox.Id)

	for {
		select {
		default:
			idleResultXmlStr := imap.IdleCheck(30000)
			if imap.LastMethodSuccess() != true {
				errorAt := time.Now()

				if _, err := pool.Exec(context.Background(), `UPDATE mails SET error_msg = $1, error_at = $2 WHERE id = $3`, imap.LastErrorXml(), errorAt, *mailbox.Id); err != nil {
					log.Error().
						Err(err).
						Int("mail", *mailbox.Id).
						Msg("Wrong update mail status error")
				}

				//log.Error().
				//	Str("mailbox", *mailbox.ImapUser).
				//	//Str("error", imap.LastErrorXml()).
				//	Msg("Failed IdleCheck")
				if !connectIMAP(imap, mailbox) {
					//log.Error().
					//	Str("mailbox", *mailbox.ImapUser).
					//	Str("error", imap.LastErrorText()).
					//	Msg("Failed connectIMAP")
					controlChan <- fmt.Sprintf("delete %d", *mailbox.Id)
					return
				}

				success = imap.IdleStart()
				if !success {
					if _, err := pool.Exec(context.Background(), `UPDATE mails SET status = 'RESTART IDLE FAILED' WHERE id = $1`, *mailbox.Id); err != nil {
						log.Error().
							Err(err).
							Int("mail", *mailbox.Id).
							Msg("Failed update mail status")
					}
					//log.Error().
					//	Str("mailbox", *mailbox.ImapUser).
					//	Str("error", imap.LastErrorText()).
					//	Msg("Failed IdleStart")
					controlChan <- fmt.Sprintf("delete %d", *mailbox.Id)
					return
				}
				//log.Info().
				//	Str("mailbox", *mailbox.ImapUser).
				//	//Str("error", imap.LastErrorText()).
				//	Msg("Restarted Idle")
				continue
			} else {
				//
				//log.Info().
				//	Str("mailbox", *mailbox.ImapUser).
				//	Msg("Succes IdleCheck")
			}
			// Проверяем, не пустой ли XML
			if *idleResultXmlStr != "<idle></idle>" {
				// Загружаем XML
				success := xml.LoadXml(*idleResultXmlStr)
				if !success {
					log.Error().
						Str("mailbox", *mailbox.ImapUser).
						Str("error", xml.LastErrorXml()).
						Msg("Failed to load XML")
				} else {

				}
			} else {
				//fmt.Println("No updates from IDLE check")
			}
		}
	}
	select {}
}

func ParseProxyConfig(proxyString string) (*ProxyConfig, error) {
	parts := strings.Split(proxyString, "://")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Wrong proxy format: %s", proxyString)
	}

	proxyType := parts[0]
	var socksType int
	if proxyType == "socks5" {
		socksType = 5
	} else if proxyType == "socks4" {
		socksType = 4
	} else {
		return nil, fmt.Errorf("Unsupported proxy: %s", proxyType)
	}

	rest := parts[1]
	userInfo := strings.SplitN(rest, "@", 2)

	var user, password, hostPort string
	if len(userInfo) == 2 {
		userPass := strings.SplitN(userInfo[0], ":", 2)
		if len(userPass) == 2 {
			user = userPass[0]
			password = userPass[1]
		}
		hostPort = userInfo[1]
	} else {
		hostPort = userInfo[0]
	}

	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return nil, fmt.Errorf("Wrong host proxy: %v", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("Wron port proxy: %s", portStr)
	}

	proxyConfig := &ProxyConfig{
		Type: socksType,
		Host: host,
		Port: port,
	}

	if user != "" {
		proxyConfig.User = &user
	}
	if password != "" {
		proxyConfig.Password = &password
	}

	return proxyConfig, nil
}

func connectIMAP(imap *chilkat.Imap, mailbox Mailbox) bool {

	if *mailbox.Proxy == "" {
		if _, err := pool.Exec(context.Background(), `UPDATE mails SET status = 'NO PROXY' WHERE id = $1`, *mailbox.Id); err != nil {
			log.Error().
				Err(err).
				Int("mail", *mailbox.Id).
				Msg("Wrong update mail status error")
		}
		return false
	}

	proxyConfig, err := ParseProxyConfig(*mailbox.Proxy)
	if err != nil {
		log.Error().
			Err(err).
			Str("function", "ParseProxyConfig").
			Int("mail", *mailbox.Id).
			Msg("Wrong parse proxy")

		return false
	}

	imap.SetSocksHostname(proxyConfig.Host)
	imap.SetSocksPort(proxyConfig.Port)
	if proxyConfig.User != nil && proxyConfig.Password != nil {
		imap.SetSocksUsername(*proxyConfig.User)
		imap.SetSocksPassword(*proxyConfig.Password)
	}
	imap.SetSocksVersion(proxyConfig.Type)

	imap.SetPeekMode(true)
	imap.SetAppendSeen(false)
	imap.SetSsl(true)
	imap.SetPort(*mailbox.ImapPort)

	success := imap.Connect(*mailbox.ImapServer)
	if !success {
		updateMailStatusBasedOnError(imap.LastErrorXml(), *mailbox.Id)
		return false
	}

	success = imap.Login(*mailbox.ImapUser, *mailbox.ImapPassword)
	if !success {
		updateMailStatusBasedOnError(imap.LastErrorXml(), *mailbox.Id)
		return false
	}

	success = imap.SelectMailbox("Inbox")
	if !success {
		updateMailStatusBasedOnError(imap.LastErrorXml(), *mailbox.Id)
		return false
	}

	//if _, err := pool.Exec(context.Background(), `UPDATE mails SET status = 'LOGIN OK',error_msg = '' WHERE id = $1`, *mailbox.Id); err != nil {
	//	log.Error().
	//		Err(err).
	//		Int("mail", *mailbox.Id).
	//		Msg("Wrong update mail status error")
	//	return false
	//}

	if _, err := pool.Exec(context.Background(), `UPDATE mails SET status = 'LOGIN OK' WHERE id = $1`, *mailbox.Id); err != nil {
		log.Error().
			Err(err).
			Int("mail", *mailbox.Id).
			Msg("Wrong update mail status error")
		return false
	}

	return true
}

func updateMailStatusBasedOnError(errorText string, mailId int) {
	var status string
	errorTextLower := strings.ToLower(errorText) // Приведение текста ошибки к нижнему регистру

	if strings.Contains(errorTextLower, strings.ToLower("SOCKS5 server rejected connection request")) ||
		strings.Contains(errorTextLower, strings.ToLower("No SOCKS5 username and/or password, requesting No-Authentication")) ||
		strings.Contains(errorTextLower, strings.ToLower("Aborting handshake because of fatal alert")) ||
		strings.Contains(errorTextLower, strings.ToLower("Failed to connect to SOCKS5 server")) ||
		strings.Contains(errorTextLower, strings.ToLower("SOCKS5 server rejected username/password")) {
		status = "PROXY REJECTED CONNECTION"
	} else if strings.Contains(errorTextLower, strings.ToLower("Failed to receive method-select reply from SOCKS5 server")) {
		status = "PROXY ERROR"
	} else if strings.Contains(errorTextLower, strings.ToLower("Failed to receive response from SOCKS5 server")) ||
		strings.Contains(errorTextLower, strings.ToLower("No connection to IMAP server")) {
		status = "PROXY TIMED OUT"
	} else if strings.Contains(errorTextLower, strings.ToLower("authentication failed")) ||
		strings.Contains(errorTextLower, strings.ToLower("Authentication failed")) ||
		strings.Contains(errorTextLower, strings.ToLower("incorrect password or account")) ||
		strings.Contains(errorTextLower, strings.ToLower("Invalid login or password")) ||
		strings.Contains(errorTextLower, strings.ToLower("Invalid username or password")) {
		status = "AUTHENTICATION FAILED"

	} else if strings.Contains(errorTextLower, strings.ToLower("Failed to get next response line from IMAP server.")) ||
		strings.Contains(errorTextLower, strings.ToLower("Escaping quotes and backslashes in mailbox name")) ||
		strings.Contains(errorTextLower, strings.ToLower("NO LOGIN failed")) ||
		strings.Contains(errorTextLower, strings.ToLower("Internal error occurred.")) ||
		strings.Contains(errorTextLower, strings.ToLower("Login not permitted")) ||
		strings.Contains(errorTextLower, strings.ToLower("Temporary authentication failure")) ||
		strings.Contains(errorTextLower, strings.ToLower("No connection to IMAP server")) ||
		strings.Contains(errorTextLower, strings.ToLower("Failed to read beginning of SSL/TLS record")) ||
		strings.Contains(errorTextLower, strings.ToLower("Account is temporarily unavailable")) {

		status = "LOGIN ERROR"
	} else {
		status = "UNDEFINED ERROR"
	}

	if _, err := pool.Exec(context.Background(), `UPDATE mails SET status = $1, error_msg = $2 WHERE id = $3`, status, errorText, mailId); err != nil {
		log.Error().
			Err(err).
			Int("mailId", mailId).
			Msg("Failed to update mail status and error message")
	}
}
