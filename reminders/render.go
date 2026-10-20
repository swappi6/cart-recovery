package reminders

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"text/template"

	"cartrecovery/commons"
)

//go:embed templates.json
var templateConfig []byte

type templateVersion struct {
	Version string `json:"version"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type compiledTemplate struct {
	subject *template.Template
	body    *template.Template
}

// Renderer keeps immutable versions in config, not database rows. All reminder
// steps share the same wording in this prototype.
type Renderer struct{ versions map[string]compiledTemplate }

func DefaultRenderer() (*Renderer, error) { return NewRenderer(templateConfig) }

func NewRenderer(config []byte) (*Renderer, error) {
	var versions []templateVersion
	if err := json.Unmarshal(config, &versions); err != nil {
		return nil, err
	}
	renderer := &Renderer{versions: make(map[string]compiledTemplate)}
	for _, version := range versions {
		subject, err := template.New("subject").Parse(version.Subject)
		if err != nil {
			return nil, err
		}
		body, err := template.New("body").Parse(version.Body)
		if err != nil {
			return nil, err
		}
		renderer.versions[version.Version] = compiledTemplate{subject: subject, body: body}
	}
	return renderer, nil
}

func (r *Renderer) Render(claim commons.Claim) (commons.Message, error) {
	version, ok := r.versions[claim.Reminder.ConfigVersion]
	if !ok {
		return commons.Message{}, fmt.Errorf("template version %q is unavailable", claim.Reminder.ConfigVersion)
	}
	data := claim.Snapshot
	if data.Recipient.FirstName == "" {
		data.Recipient.FirstName = "there"
	}
	var subject, body bytes.Buffer
	if err := version.subject.Execute(&subject, data); err != nil {
		return commons.Message{}, err
	}
	if err := version.body.Execute(&body, data); err != nil {
		return commons.Message{}, err
	}
	return commons.Message{Subject: subject.String(), Body: body.String()}, nil
}
